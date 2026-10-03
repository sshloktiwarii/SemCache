#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use axum::{routing::{get, post}, Router};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use semcache::{
    coalesce::RequestCoalescer,
    db::{enforce_max_db_size, init_db_pool, prune_expired_records},
    proxy::{handle_chat_completion, handle_healthz, handle_metrics, AppState},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "semcache=debug,axum=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let raw_db_path = std::env::var("SEMCACHE_DB_PATH").unwrap_or_else(|_| "semcache.db".to_string());
    let db_path_buf = std::path::PathBuf::from(&raw_db_path);
    let absolute_db_path = if db_path_buf.is_absolute() {
        db_path_buf
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join(db_path_buf)
    };
    let db_path = absolute_db_path.to_string_lossy().to_string();

    let upstream_url = std::env::var("OPENAI_UPSTREAM_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1/chat/completions".to_string());
    
    // Default bind to loopback address (127.0.0.1:3000) for security
    let bind_addr = std::env::var("SEMCACHE_BIND").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let ttl_days: i64 = std::env::var("SEMCACHE_TTL_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);
    
    let default_tenant_id = std::env::var("SEMCACHE_TENANT_ID")
        .unwrap_or_else(|_| "default_tenant".to_string());

    let max_request_bytes: usize = std::env::var("SEMCACHE_MAX_REQUEST_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024 * 1024); // 32 MB default (supports vision models with base64 images)

    let max_response_bytes: usize = std::env::var("SEMCACHE_MAX_RESPONSE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10 * 1024 * 1024); // 10 MB default

    let max_ready_bytes: usize = std::env::var("SEMCACHE_MAX_READY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128 * 1024 * 1024); // 128 MB default for Ready RAM retention

    let max_db_bytes: u64 = std::env::var("SEMCACHE_MAX_DB_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024); // 10 GB default storage cap

    let max_upstream_concurrency: usize = std::env::var("SEMCACHE_MAX_UPSTREAM_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);
    let upstream_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_upstream_concurrency));

    let cancel_orphan_requests = std::env::var("SEMCACHE_CANCEL_ORPHAN_REQUESTS")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    let deterministic_only = std::env::var("SEMCACHE_DETERMINISTIC_ONLY")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    let credential_headers: Vec<String> = std::env::var("SEMCACHE_CREDENTIAL_HEADERS")
        .map(|s| s.split(',').map(|h| h.trim().to_ascii_lowercase()).filter(|h| !h.is_empty()).collect())
        .unwrap_or_else(|_| vec![
            "authorization".to_string(),
            "api-key".to_string(),
            "x-api-key".to_string(),
            "x-goog-api-key".to_string(),
        ]);

    let upstream_timeout_secs: u64 = std::env::var("SEMCACHE_UPSTREAM_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    info!("Initializing SQLite WAL persistence layer at absolute path '{}'...", db_path);
    let pool = init_db_pool(&db_path)?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(upstream_timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(64)
        .build()?;

    let coalescer = RequestCoalescer::with_max_ready_bytes(max_ready_bytes);

    // Background sweep task for expired Ready entries in InFlightMap (every 10s)
    let coalescer_sweep = coalescer.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let swept = coalescer_sweep.sweep_stale_ready(Duration::from_secs(10));
            if swept > 0 {
                tracing::debug!("Swept {} stale ready entries from InFlightMap (current ready bytes: {})", swept, coalescer_sweep.ready_bytes());
            }
        }
    });

    // Background TTL and DB size cap maintenance task running hourly
    let prune_pool = pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            info!("Running background cache TTL eviction (TTL: {} days)...", ttl_days);
            let pool_task = prune_pool.clone();
            let res = tokio::task::spawn_blocking(move || {
                let deleted = prune_expired_records(&pool_task, ttl_days)?;
                let capped = enforce_max_db_size(&pool_task, max_db_bytes)?;
                Ok::<_, semcache::error::SemCacheError>((deleted, capped))
            }).await;

            match res {
                Ok(Ok((deleted, capped))) => {
                    if deleted > 0 {
                        info!("Pruned {} expired cache records from SQLite.", deleted);
                    }
                    if capped > 0 {
                        info!("Pruned {} oldest records to enforce {} bytes DB cap.", capped, max_db_bytes);
                    }
                }
                Ok(Err(e)) => error!("TTL or storage cap maintenance failed: {}", e),
                Err(e) => error!("Maintenance task panicked: {}", e),
            }
        }
    });

    let max_concurrent_writes: usize = std::env::var("SEMCACHE_MAX_CONCURRENT_WRITES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let sqlite_write_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent_writes));

    let default_provider_str = std::env::var("SEMCACHE_DEFAULT_PROVIDER")
        .unwrap_or_else(|_| "openai".to_string());
    let default_provider = match default_provider_str.to_ascii_lowercase().trim() {
        "ollama" => semcache::canonical::Provider::Ollama,
        "generic" | "raw" => semcache::canonical::Provider::Generic,
        _ => semcache::canonical::Provider::OpenAi,
    };

    let requests_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let l1_hits_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let coalesced_hits_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let upstream_fetches_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let streaming_bypasses_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let oversized_bypasses_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stochastic_bypasses_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let upstream_shed_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let dropped_writes_total = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let consecutive_write_failures = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let state = AppState {
        db: pool,
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
        upstream_semaphore,
        default_provider,
        default_tenant_id,
        max_request_bytes,
        max_response_bytes,
        cancel_orphan_requests,
        deterministic_only,
        credential_headers,
        requests_total: requests_total.clone(),
        l1_hits_total: l1_hits_total.clone(),
        coalesced_hits_total: coalesced_hits_total.clone(),
        upstream_fetches_total: upstream_fetches_total.clone(),
        streaming_bypasses_total: streaming_bypasses_total.clone(),
        oversized_bypasses_total: oversized_bypasses_total.clone(),
        stochastic_bypasses_total: stochastic_bypasses_total.clone(),
        upstream_shed_total: upstream_shed_total.clone(),
        dropped_writes_total: dropped_writes_total.clone(),
        consecutive_write_failures: consecutive_write_failures.clone(),
    };

    // Periodic telemetry log line (every 60s)
    let telemetry_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            info!(
                "SemCache Telemetry: reqs={}, hits_l1={}, coalesced={}, upstream={}, shed={}, dropped_writes={}, breaker_open={}",
                telemetry_state.requests_total.load(Ordering::Relaxed),
                telemetry_state.l1_hits_total.load(Ordering::Relaxed),
                telemetry_state.coalesced_hits_total.load(Ordering::Relaxed),
                telemetry_state.upstream_fetches_total.load(Ordering::Relaxed),
                telemetry_state.upstream_shed_total.load(Ordering::Relaxed),
                telemetry_state.dropped_writes_total.load(Ordering::Relaxed),
                telemetry_state.consecutive_write_failures.load(Ordering::Relaxed) >= 5,
            );
        }
    });

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .route("/healthz", get(handle_healthz))
        .route("/metrics", get(handle_metrics))
        .with_state(state);

    if bind_addr.starts_with("127.0.0.1") {
        info!("Notice: Bound to loopback interface ({}). In Docker environments, set SEMCACHE_BIND=0.0.0.0:3000 behind a secure reverse proxy.", bind_addr);
    }

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("🚀 SemCache Vector-Similarity Gateway active on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("SemCache gateway shutdown cleanly.");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!("Failed to install Ctrl+C handler: {}", e);
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::warn!("Failed to install SIGTERM handler: {}", e);
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C (SIGINT); initiating graceful shutdown...");
        },
        _ = terminate => {
            tracing::info!("Received SIGTERM; initiating graceful shutdown...");
        },
    }
}


use axum::{routing::{get, post}, Router};
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use semcache::{
    coalesce::RequestCoalescer,
    db::{init_db_pool, prune_expired_records},
    proxy::{handle_chat_completion, handle_healthz, AppState},
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

    let db_path = std::env::var("SEMCACHE_DB_PATH").unwrap_or_else(|_| "semcache.db".to_string());
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

    let max_upstream_concurrency: usize = std::env::var("SEMCACHE_MAX_UPSTREAM_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);
    let upstream_semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_upstream_concurrency));

    let cancel_orphan_requests = std::env::var("SEMCACHE_CANCEL_ORPHAN_REQUESTS")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    let upstream_timeout_secs: u64 = std::env::var("SEMCACHE_UPSTREAM_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    let canonical_db_path = std::path::Path::new(&db_path)
        .canonicalize()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| db_path.clone());
    info!("Initializing SQLite WAL persistence layer at '{}'...", canonical_db_path);
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

    // Background TTL pruning task running hourly
    let prune_pool = pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            info!("Running background cache TTL eviction (TTL: {} days)...", ttl_days);
            let pool_task = prune_pool.clone();
            let res = tokio::task::spawn_blocking(move || {
                prune_expired_records(&pool_task, ttl_days)
            }).await;

            match res {
                Ok(Ok(deleted)) => {
                    if deleted > 0 {
                        info!("Pruned {} expired cache records from SQLite.", deleted);
                    }
                }
                Ok(Err(e)) => error!("TTL eviction failed: {}", e),
                Err(e) => error!("Eviction task panicked: {}", e),
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
        upstream_shed_total,
        dropped_writes_total,
        consecutive_write_failures,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .route("/healthz", get(handle_healthz))
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
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
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

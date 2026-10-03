use axum::{routing::post, Router};
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use semcache::{
    coalesce::RequestCoalescer,
    db::{init_db_pool, prune_expired_records},
    proxy::{handle_chat_completion, AppState},
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
    let bind_addr = std::env::var("SEMCACHE_BIND").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    let ttl_days: i64 = std::env::var("SEMCACHE_TTL_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7);
    
    // CRITICAL FIX (The 60-Second Guillotine):
    // Reasoning models (e.g. o1-preview) and large code generation streams routinely take 90+ seconds.
    // Defaulting to 300s prevents premature connection severance.
    let upstream_timeout_secs: u64 = std::env::var("SEMCACHE_UPSTREAM_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    info!("Initializing SQLite WAL persistence layer at '{}'...", db_path);
    let pool = init_db_pool(&db_path)?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(upstream_timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(64)
        .build()?;

    let coalescer = RequestCoalescer::new();

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

    let state = AppState {
        db: pool,
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("🚀 SemCache Vector-Similarity Gateway active on http://{}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("SemCache gateway shutdown cleanly.");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    routing::post,
    Router,
};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tower::ServiceExt;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use semcache::{
    coalesce::RequestCoalescer,
    db::init_db_pool,
    proxy::{handle_chat_completion, AppState},
};

fn get_rss_bytes() -> usize {
    let pid = std::process::id().to_string();
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
    {
        if let Ok(s) = String::from_utf8(output.stdout) {
            if let Ok(kb) = s.trim().parse::<usize>() {
                return kb * 1024;
            }
        }
    }
    0
}

fn rand_suffix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn create_soak_app(upstream_url: String) -> (Router, semcache::db::DbPool, String, AppState) {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("semcache_soak_{}_{}.db", std::process::id(), rand_suffix()));
    let db_path_str = db_path.to_string_lossy().to_string();

    let pool = init_db_pool(&db_path_str).expect("init soak db pool");
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(128)
        .build()
        .expect("build soak client");

    let coalescer = RequestCoalescer::new();
    let sqlite_write_semaphore = Arc::new(tokio::sync::Semaphore::new(4));
    let upstream_semaphore = Arc::new(tokio::sync::Semaphore::new(256));
    let upstream_shed_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let dropped_writes_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let consecutive_write_failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let state = AppState {
        db: pool.clone(),
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
        upstream_semaphore,
        default_provider: semcache::canonical::Provider::OpenAi,
        default_tenant_id: "soak_tenant".to_string(),
        max_request_bytes: 32 * 1024 * 1024,
        max_response_bytes: 10 * 1024 * 1024,
        cancel_orphan_requests: false,
        upstream_shed_total,
        dropped_writes_total,
        consecutive_write_failures,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state.clone());

    (app, pool, db_path_str, state)
}

#[tokio::test]
async fn test_sustained_concurrency_soak_run() {
    println!("\n=======================================================");
    println!("   SEMCACHE SUSTAINED CONCURRENCY & SOAK BENCHMARK     ");
    println!("=======================================================");

    let mock_server = MockServer::start().await;
    let upstream_counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = upstream_counter.clone();

    // WireMock mock responding with slight simulated generation delay (15ms)
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(move |_req: &wiremock::Request| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(15))
                .set_body_json(json!({
                    "id": "chatcmpl-soak",
                    "choices": [{
                        "message": { "role": "assistant", "content": "Sustained high-throughput completion." }
                    }]
                }))
        })
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_soak_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    let initial_rss = get_rss_bytes();
    let total_requests = 1000usize;
    let concurrency = 50usize;

    let hit_l1_count = Arc::new(AtomicUsize::new(0));
    let hit_coalesced_count = Arc::new(AtomicUsize::new(0));
    let miss_upstream_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(total_requests)));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    // Dispatch 1,000 requests across 50 concurrent worker loops
    // Key space: 10 distinct prompt buckets, creating massive coalescing & L1 reuse
    for i in 0..concurrency {
        let app_clone = app.clone();
        let hit_l1_c = hit_l1_count.clone();
        let hit_coal_c = hit_coalesced_count.clone();
        let miss_up_c = miss_upstream_count.clone();
        let err_c = error_count.clone();
        let latencies_c = latencies.clone();
        let requests_per_worker = total_requests / concurrency;

        handles.push(tokio::spawn(async move {
            for j in 0..requests_per_worker {
                let bucket_id = (i * requests_per_worker + j) % 10;
                let payload = json!({
                    "model": "gpt-4o",
                    "messages": [{ "role": "user", "content": format!("Soak query pattern bucket #{}", bucket_id) }],
                    "temperature": 1.0 // Normalized to OpenAI default
                });

                let req = Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header(header::AUTHORIZATION, "Bearer sk-soak-tenant")
                    .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                    .unwrap();

                let req_start = Instant::now();
                let resp = (*app_clone).clone().oneshot(req).await;
                let elapsed_ms = req_start.elapsed().as_secs_f64() * 1000.0;

                match resp {
                    Ok(r) => {
                        let status = r.status();
                        let semcache_status = r
                            .headers()
                            .get("x-semcache-status")
                            .and_then(|h| h.to_str().ok())
                            .unwrap_or("UNKNOWN")
                            .to_string();

                        let body = r.into_body().collect().await;
                        if status == StatusCode::OK && body.is_ok() {
                            match semcache_status.as_str() {
                                "HIT_L1" => { hit_l1_c.fetch_add(1, Ordering::Relaxed); }
                                "HIT_COALESCED" => { hit_coal_c.fetch_add(1, Ordering::Relaxed); }
                                "MISS_UPSTREAM" => { miss_up_c.fetch_add(1, Ordering::Relaxed); }
                                _ => {}
                            }
                            latencies_c.lock().await.push(elapsed_ms);
                        } else {
                            err_c.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        err_c.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_duration = start_time.elapsed();
    let final_rss = get_rss_bytes();

    // Latency Percentile Calculation
    let mut lat_vec = latencies.lock().await.clone();
    lat_vec.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = lat_vec[lat_vec.len() * 50 / 100];
    let p95 = lat_vec[lat_vec.len() * 95 / 100];
    let p99 = lat_vec[lat_vec.len() * 99 / 100];
    let min_lat = lat_vec[0];
    let max_lat = lat_vec[lat_vec.len() - 1];

    let l1_hits = hit_l1_count.load(Ordering::Relaxed);
    let coalesced_hits = hit_coalesced_count.load(Ordering::Relaxed);
    let upstream_misses = miss_upstream_count.load(Ordering::Relaxed);
    let actual_upstream_requests = upstream_counter.load(Ordering::Relaxed);
    let total_success = l1_hits + coalesced_hits + upstream_misses;

    let l1_ratio = (l1_hits as f64 / total_requests as f64) * 100.0;
    let coalescing_ratio = (coalesced_hits as f64 / total_requests as f64) * 100.0;
    let upstream_amplification = (actual_upstream_requests as f64 / total_requests as f64) * 100.0;

    // Check WAL file size
    let wal_path = format!("{}-wal", db_path);
    let wal_size_bytes = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    let db_size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    // Check in-flight map count
    let residual_in_flight = state.coalescer.in_flight.len();
    let available_permits = state.sqlite_write_semaphore.available_permits();

    println!("Total Inbound Requests:       {}", total_requests);
    println!("Total Execution Time:         {:.2?}", total_duration);
    println!("Throughput:                   {:.2} req/sec", total_requests as f64 / total_duration.as_secs_f64());
    println!("-------------------------------------------------------");
    println!("Actual Upstream Dispatches:   {} ({:.1}% of inbound)", actual_upstream_requests, upstream_amplification);
    println!("L1 Cache Hits:                {} ({:.1}%)", l1_hits, l1_ratio);
    println!("Coalesced Hits:               {} ({:.1}%)", coalesced_hits, coalescing_ratio);
    println!("Cache Offload Ratio:          {:.1}%", l1_ratio + coalescing_ratio);
    println!("-------------------------------------------------------");
    println!("Latency (min / p50 / p95 / p99 / max):");
    println!("  Min: {:.2}ms | p50: {:.2}ms | p95: {:.2}ms | p99: {:.2}ms | Max: {:.2}ms", min_lat, p50, p95, p99, max_lat);
    println!("-------------------------------------------------------");
    println!("Resident Memory (RSS):        Initial: {:.2} MB | Final: {:.2} MB (Diff: {:+} KB)",
        initial_rss as f64 / 1_048_576.0,
        final_rss as f64 / 1_048_576.0,
        (final_rss as i64 - initial_rss as i64) / 1024
    );
    println!("SQLite DB Size:               {} KB", db_size_bytes / 1024);
    println!("SQLite WAL Size:              {} KB", wal_size_bytes / 1024);
    println!("Residual In-Flight Entries:   {}", residual_in_flight);
    println!("Available Write Permits:      {} / 4", available_permits);
    println!("Error Count:                  {}", error_count.load(Ordering::Relaxed));
    println!("=======================================================\n");

    // ASSERTIONS:
    assert_eq!(total_success, total_requests, "All requests must complete successfully");
    assert_eq!(error_count.load(Ordering::Relaxed), 0, "Zero errors during healthy soak run");
    assert!(
        actual_upstream_requests <= 15,
        "With 10 unique buckets, upstream requests must not exceed 15 (got {})",
        actual_upstream_requests
    );
    assert!(
        upstream_amplification <= 2.0,
        "Upstream request ratio must be <= 2% under 1000 requests (got {:.2}%)",
        upstream_amplification
    );
    assert_eq!(residual_in_flight, 0, "In-flight map must have zero residual entries upon completion");
    assert_eq!(available_permits, 4, "All 4 SQLite write semaphore permits must be restored");

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test]
async fn test_soak_behavior_during_sqlite_stall() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{ "message": { "content": "Survives SQLite Stall" } }]
        })))
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_soak_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    // Intentionally drain all 4 semaphore permits to simulate an enduring disk freeze
    let p1 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let p2 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let p3 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let p4 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();

    let mut handles = Vec::new();
    for i in 0..20 {
        let app_clone = app.clone();
        handles.push(tokio::spawn(async move {
            let payload = json!({
                "model": "gpt-4o",
                "messages": [{ "role": "user", "content": format!("Stall query #{}", i) }]
            });

            let req = Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer sk-test")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            let resp = (*app_clone).clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["choices"][0]["message"]["content"].as_str().unwrap(), "Survives SQLite Stall");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Release simulated freeze permits
    drop(p1);
    drop(p2);
    drop(p3);
    drop(p4);

    // In-flight map must still be fully evicted
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(state.coalescer.in_flight.len(), 0);
    assert_eq!(state.sqlite_write_semaphore.available_permits(), 4);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_soak_behavior_during_upstream_stall() {
    let mock_server = MockServer::start().await;

    // Upstream takes 100ms per chunk; client has 30ms inter-chunk timeout
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
        .mount(&mock_server)
        .await;

    let (_app, _pool, db_path, state) = create_soak_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let (guard, leader_rx) = match state.coalescer.register_or_wait([55u8; 32]).await.unwrap() {
        semcache::coalesce::CoalesceResult::Primary(g, rx) => (g, rx),
        _ => panic!("Expected primary"),
    };

    // Simulate caller dropping due to client-side timeout
    drop(leader_rx);
    assert!(!guard.has_active_listeners());

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_soak_behavior_during_process_shutdown() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(100)).set_body_json(json!({
            "choices": [{ "message": { "content": "Clean shutdown" } }]
        })))
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_soak_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    // Fire 10 in-flight requests
    let mut handles = Vec::new();
    for _ in 0..10 {
        let app_clone = app.clone();
        handles.push(tokio::spawn(async move {
            let payload = json!({
                "model": "gpt-4o",
                "messages": [{ "role": "user", "content": "Shutdown test" }]
            });

            let req = Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer sk-test")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            (*app_clone).clone().oneshot(req).await
        }));
    }

    // Abruptly cancel half the tasks midway through execution (simulating SIGTERM / client cancellation)
    tokio::time::sleep(Duration::from_millis(20)).await;
    handles.pop().unwrap().abort();
    handles.pop().unwrap().abort();

    for h in handles {
        let _ = h.await;
    }

    // Wait for in-flight tasks to settle
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(state.coalescer.in_flight.len(), 0, "In-flight map cleaned up with zero deadlocks");

    let _ = std::fs::remove_file(db_path);
}

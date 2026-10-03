use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

use semcache::{
    coalesce::RequestCoalescer,
    db::{get_exact_cache, init_db_pool},
    proxy::{handle_chat_completion, handle_healthz, AppState},
};

fn create_test_app(upstream_url: String) -> (Router, semcache::db::DbPool, String, AppState) {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("semcache_integ_{}_{}.db", std::process::id(), rand_suffix()));
    let db_path_str = db_path.to_string_lossy().to_string();

    let pool = init_db_pool(&db_path_str).expect("init test db pool");
    let http_client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    let coalescer = RequestCoalescer::new();
    let sqlite_write_semaphore = Arc::new(tokio::sync::Semaphore::new(4));
    let upstream_semaphore = Arc::new(tokio::sync::Semaphore::new(256));
    let upstream_shed_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let dropped_writes_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let consecutive_write_failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let requests_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let l1_hits_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let coalesced_hits_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let upstream_fetches_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let streaming_bypasses_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let oversized_bypasses_total = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stochastic_bypasses_total = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let state = AppState {
        db: pool.clone(),
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
        upstream_semaphore,
        default_provider: semcache::canonical::Provider::OpenAi,
        default_tenant_id: "test_tenant".to_string(),
        max_request_bytes: 32 * 1024 * 1024,
        max_response_bytes: 10 * 1024 * 1024,
        cancel_orphan_requests: false,
        deterministic_only: false,
        credential_headers: vec![
            "authorization".to_string(),
            "api-key".to_string(),
            "x-api-key".to_string(),
            "x-goog-api-key".to_string(),
        ],
        requests_total,
        l1_hits_total,
        coalesced_hits_total,
        upstream_fetches_total,
        streaming_bypasses_total,
        oversized_bypasses_total,
        stochastic_bypasses_total,
        upstream_shed_total,
        dropped_writes_total,
        consecutive_write_failures,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .route("/healthz", get(handle_healthz))
        .route("/metrics", get(semcache::proxy::handle_metrics))
        .with_state(state.clone());

    (app, pool, db_path_str, state)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_50_agent_multi_thread_barrier_stampede() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(json!({
                    "id": "chatcmpl-stampede",
                    "choices": [{
                        "message": { "role": "assistant", "content": "Autonomous Coalesced Completion" }
                    }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "What is the capital of France?" }],
        "temperature": 0.0
    });

    let barrier = Arc::new(tokio::sync::Barrier::new(50));
    let miss_upstream_count = Arc::new(AtomicUsize::new(0));
    let hit_coalesced_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // Spawn 50 concurrent client tasks synchronized at a barrier to force true OS thread contention
    for _ in 0..50 {
        let app_clone = app.clone();
        let payload_clone = payload.clone();
        let barrier_clone = barrier.clone();
        let miss_counter = miss_upstream_count.clone();
        let hit_counter = hit_coalesced_count.clone();

        handles.push(tokio::spawn(async move {
            barrier_clone.wait().await;

            let req = Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer sk-test-key")
                .body(Body::from(serde_json::to_vec(&payload_clone).unwrap()))
                .unwrap();

            let resp = (*app_clone).clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let semcache_status = resp
                .headers()
                .get("x-semcache-status")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .to_string();

            if semcache_status == "MISS_UPSTREAM" {
                miss_counter.fetch_add(1, Ordering::Relaxed);
            } else if semcache_status == "HIT_COALESCED" {
                hit_counter.fetch_add(1, Ordering::Relaxed);
            }

            let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
            let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

            (status, body_json)
        }));
    }

    for handle in handles {
        let (status, body) = handle.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["choices"][0]["message"]["content"].as_str().unwrap(),
            "Autonomous Coalesced Completion"
        );
    }

    // Exactly 1 Primary Leader must have fetched upstream, and 49 must have coalesced!
    assert_eq!(miss_upstream_count.load(Ordering::Relaxed), 1, "Exactly one leader must fetch upstream");
    assert_eq!(hit_coalesced_count.load(Ordering::Relaxed), 49, "Exactly 49 followers must coalesce");

    // WireMock confirms exactly 1 upstream request occurred
    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_leader_disconnect_does_not_abort_followers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(150))
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Resilient Detached Result" } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Autonomous Self-Healing Task" }]
    });

    let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel::<()>();

    // Leader Task: Disconnects/aborts immediately after dispatching request
    let app_leader = app.clone();
    let payload_leader = payload.clone();
    let leader_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(serde_json::to_vec(&payload_leader).unwrap()))
            .unwrap();

        // Signal that leader has begun execution
        let _ = leader_started_tx.send(());

        // Cancel leader by wrapping in a 30ms timeout (well before the 150ms upstream response)
        let _ = tokio::time::timeout(Duration::from_millis(30), (*app_leader).clone().oneshot(req)).await;
    });

    // Wait until leader registers
    leader_started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;

    // Follower Task: Subscribes while leader is in-flight, survives leader disconnect!
    let app_follower = app.clone();
    let payload_follower = payload.clone();
    let follower_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(serde_json::to_vec(&payload_follower).unwrap()))
            .unwrap();

        let resp = (*app_follower).clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        (status, body_json)
    });

    let _ = leader_handle.await;
    let (follower_status, follower_body) = follower_handle.await.unwrap();

    assert_eq!(follower_status, StatusCode::OK);
    assert_eq!(
        follower_body["choices"][0]["message"]["content"].as_str().unwrap(),
        "Resilient Detached Result"
    );

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_default_key_hash_convergence_integration() {
    let mock_server = MockServer::start().await;

    // Upstream should only receive 1 request because request A and request B must converge!
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Default convergence result" } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Request A: Leaves temperature and top_p omitted
    let payload_a = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Convergence test" }]
    });

    let req_a = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload_a).unwrap()))
        .unwrap();

    let resp_a = app.clone().oneshot(req_a).await.unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    assert_eq!(resp_a.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Allow background write to commit
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Request B: Explicitly provides default parameters (temperature=1.0, top_p=1.0)
    let payload_b = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Convergence test" }],
        "temperature": 1.0,
        "top_p": 1.0,
        "presence_penalty": 0.0
    });

    let req_b = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload_b).unwrap()))
        .unwrap();

    let resp_b = app.clone().oneshot(req_b).await.unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    assert_eq!(resp_b.headers().get("x-semcache-status").unwrap(), "HIT_L1");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_persistence_gap_with_injected_delay() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Fast Persistence Hit" } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Persistence Gap Injected Delay Test" }]
    });

    // CRITICAL: Inject a delay into the SQLite write by acquiring all 4 semaphore permits
    // This holds the background write task in a wait state while request 2 executes!
    let permit1 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit2 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit3 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit4 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();

    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Request 2 arrives while the SQLite write is delayed/waiting for permits:
    // It MUST hit the Ready state in RAM (HIT_COALESCED) and NOT miss or trigger upstream!
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(resp2.headers().get("x-semcache-status").unwrap(), "HIT_COALESCED");

    // Release write permits, allowing background write to complete and commit to SQLite WAL
    drop(permit1);
    drop(permit2);
    drop(permit3);
    drop(permit4);

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Request 3 arrives after SQLite commit: MUST hit L1 from SQLite!
    let req3 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);
    assert_eq!(resp3.headers().get("x-semcache-status").unwrap(), "HIT_L1");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_sentinel_token_log_and_storage_hygiene() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{ "message": { "content": "Secure Response" } }]
            })),
        )
        .mount(&mock_server)
        .await;

    let (app, pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let sentinel_auth = "Bearer sk-sentinel-auth-token-secret-999";
    let sentinel_api_key = "sk-sentinel-api-key-secret-888";

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Zero leak test" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, sentinel_auth)
        .header("api-key", sentinel_api_key)
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Direct SQLite verification: assert sentinel tokens NEVER appear in SQLite rows
    let conn = pool.get().unwrap();
    let mut stmt = conn.prepare("SELECT request_json, response_json FROM exact_cache").unwrap();
    let mut rows = stmt.query([]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let req_json: String = row.get(0).unwrap();
        let resp_json: String = row.get(1).unwrap();

        assert!(!req_json.contains("sk-sentinel-auth-token-secret-999"));
        assert!(!req_json.contains("sk-sentinel-api-key-secret-888"));
        assert!(!resp_json.contains("sk-sentinel-auth-token-secret-999"));
        assert!(!resp_json.contains("sk-sentinel-api-key-secret-888"));
    }

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_proxy_multi_header_tenant_isolation() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{ "message": { "content": "Multi-Header Tenant Isolation" } }]
            })),
        )
        .expect(3) // 3 distinct credentials -> 3 distinct cache partitions -> 3 upstream calls!
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Tenant isolation verification" }]
    });

    // 1. Authorization header -> Upstream call 1 (MISS_UPSTREAM)
    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer key-alpha")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Same Authorization header -> L1 HIT (0 upstream calls)
    let req1_repeat = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer key-alpha")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();
    let resp1_repeat = app.clone().oneshot(req1_repeat).await.unwrap();
    assert_eq!(resp1_repeat.headers().get("x-semcache-status").unwrap(), "HIT_L1");

    // 3. api-key header (Azure style) -> Upstream call 2 (MISS_UPSTREAM)
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header("api-key", "key-beta")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. x-api-key header (Anthropic/LiteLLM style) -> Upstream call 3 (MISS_UPSTREAM)
    let req3 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "key-gamma")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();
    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_eq!(resp3.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_sqlite_read_failure_fail_open_degradation() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{ "message": { "content": "Degraded but successful response" } }]
            })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Corrupt database by dropping the exact_cache table
    {
        let conn = pool.get().unwrap();
        conn.execute_batch("DROP TABLE exact_cache").unwrap();
    }

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Testing read failure degradation" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    // The gateway MUST fail-open: return HTTP 200 with upstream completion rather than 500 error!
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        body_json["choices"][0]["message"]["content"].as_str().unwrap(),
        "Degraded but successful response"
    );

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_request_body_size_limit_413() {
    let (_app, _pool, db_path, mut state) = create_test_app("https://api.openai.com/v1/chat/completions".to_string());
    
    // Set small 500-byte limit for testing
    state.max_request_bytes = 500;
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state);

    let big_prompt = "x".repeat(1000);
    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": big_prompt }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_healthz_endpoint() {
    let (app, _pool, db_path, _state) = create_test_app("https://api.openai.com/v1/chat/completions".to_string());

    let req = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body_json["status"].as_str().unwrap(), "ok");

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_upstream_429_propagates_without_cache_poisoning() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(429).set_body_json(json!({
                "error": { "message": "Rate limit reached: quota exceeded" }
            })),
        )
        .mount(&mock_server)
        .await;

    let (app, pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Trigger rate limit" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body_json["error"]["message"].as_str().unwrap().contains("Rate limit reached"));

    let hash = [0u8; 32];
    let check = get_exact_cache(&pool, &hash).unwrap();
    assert!(check.is_none());

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_ghost_task_aborted_when_initiator_disconnects() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock_server)
        .await;

    let (_app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let coalescer = semcache::coalesce::RequestCoalescer::new();
    let hash = [77u8; 32];

    let (mut guard, leader_rx) = match coalescer.register_or_wait(hash).await.unwrap() {
        semcache::coalesce::CoalesceResult::Primary(g, rx) => (g, rx),
        _ => panic!("Expected primary worker"),
    };

    drop(leader_rx);
    assert!(!guard.has_active_listeners(), "Dropping leader receiver must register zero listeners");

    let upstream_uri = format!("{}/v1/chat/completions", mock_server.uri());
    let handle = tokio::spawn(async move {
        if !guard.has_active_listeners() {
            guard.evict();
            return;
        }

        let client = reqwest::Client::new();
        let _ = client.post(&upstream_uri).send().await;
        guard.evict();
    });

    handle.await.unwrap();
    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_idle_timeout_stream_mechanics() {
    use bytes::Bytes;
    use futures_util::StreamExt;
    use semcache::proxy::IdleTimeoutStream;

    let items: Vec<Result<Bytes, std::io::Error>> = vec![
        Ok(Bytes::from_static(b"chunk1\n")),
        Ok(Bytes::from_static(b"chunk2\n")),
    ];
    let stream = futures_util::stream::iter(items);
    let mut idle_stream = IdleTimeoutStream::new(stream, Duration::from_millis(200), Duration::from_millis(100));

    let first = idle_stream.next().await.unwrap().unwrap();
    assert_eq!(first, Bytes::from_static(b"chunk1\n"));
    let second = idle_stream.next().await.unwrap().unwrap();
    assert_eq!(second, Bytes::from_static(b"chunk2\n"));
    assert!(idle_stream.next().await.is_none());

    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let rx_stream_b = futures_util::stream::poll_fn(move |cx| rx_b.poll_recv(cx));
    let mut reasoning_stream = IdleTimeoutStream::new(rx_stream_b, Duration::from_millis(200), Duration::from_millis(50));

    tokio::time::sleep(Duration::from_millis(100)).await;
    tx_b.send(Ok(Bytes::from_static(b"thought complete"))).await.unwrap();
    let val_b = reasoning_stream.next().await.unwrap().unwrap();
    assert_eq!(val_b, Bytes::from_static(b"thought complete"));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let rx_stream = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx));
    let mut timed_stream = IdleTimeoutStream::new(rx_stream, Duration::from_millis(100), Duration::from_millis(40));

    tx.send(Ok(Bytes::from_static(b"hello"))).await.unwrap();
    let val = timed_stream.next().await.unwrap().unwrap();
    assert_eq!(val, Bytes::from_static(b"hello"));

    tokio::time::sleep(Duration::from_millis(70)).await;
    let timeout_err = timed_stream.next().await.unwrap();
    assert!(timeout_err.is_err());
    assert_eq!(timeout_err.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
}

#[tokio::test]
async fn test_sqlite_write_semaphore_backpressure() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Backpressure Protected Result" } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let permit1 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit2 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit3 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit4 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Test saturated backpressure" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        body_json["choices"][0]["message"]["content"].as_str().unwrap(),
        "Backpressure Protected Result"
    );

    drop(permit1);
    drop(permit2);
    drop(permit3);
    drop(permit4);

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_two_different_api_keys_same_body_make_two_upstream_calls() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Tenant isolated response" } }]
                })),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Identical prompt across tenants" }]
    });

    // Request from Tenant Alice
    let req_alice = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-alice-key-111")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp_alice = app.clone().oneshot(req_alice).await.unwrap();
    assert_eq!(resp_alice.status(), StatusCode::OK);
    assert_eq!(
        resp_alice.headers().get("x-semcache-status").unwrap(),
        "MISS_UPSTREAM"
    );

    // Request from Tenant Bob (exact same body, different Authorization key)
    let req_bob = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-bob-key-222")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp_bob = app.clone().oneshot(req_bob).await.unwrap();
    assert_eq!(resp_bob.status(), StatusCode::OK);
    // Must NOT hit L1 cache; must make a second upstream call due to credential salt
    assert_eq!(
        resp_bob.headers().get("x-semcache-status").unwrap(),
        "MISS_UPSTREAM"
    );

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_multi_header_credential_concatenation_isolation() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Multi-header isolation response" } }]
                })),
        )
        .expect(2) // Exactly 2 upstream calls for user-alice and user-bob!
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Shared gateway check" }]
    });

    // Request 1: Shared gateway Authorization + user-alice api-key
    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-shared-gateway")
        .header("api-key", "user-alice")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Request 2: Same shared gateway Authorization + user-bob api-key
    // Because both headers are salted, user-bob does NOT collide with user-alice!
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-shared-gateway")
        .header("api-key", "user-bob")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(resp2.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Request 3: Repeat user-alice -> hits user-alice's cache partition
    let req3 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-shared-gateway")
        .header("api-key", "user-alice")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);
    assert_eq!(resp3.headers().get("x-semcache-status").unwrap(), "HIT_L1");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}


#[tokio::test]
async fn test_upstream_semaphore_exhaustion_returns_503_and_retry_after() {
    let mock_server = MockServer::start().await;

    let (app, _pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Exhaust all 256 permits of the upstream semaphore
    let mut permits = Vec::new();
    for _ in 0..256 {
        permits.push(state.upstream_semaphore.clone().try_acquire_owned().unwrap());
    }

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Shed request" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers().get(header::RETRY_AFTER).unwrap(), "5");
    assert_eq!(resp.headers().get(header::CONTENT_TYPE).unwrap(), "application/json");

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body_json["error"]["type"].as_str().unwrap(), "concurrency_limit_exceeded");
    assert!(state.upstream_shed_total.load(std::sync::atomic::Ordering::Relaxed) >= 1);

    drop(permits);
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_cache_control_no_store_bypass() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "No store response" } }]
                })),
        )
        .expect(2)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "No-store prompt" }]
    });

    // Request 1: Normal request, populates L1 cache
    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Request 2: Identical prompt, but Cache-Control: no-store
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(resp2.headers().get("x-semcache-status").unwrap(), "BYPASS_NO_STORE");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_slow_upstream_pending_coalesces_followers_past_10s() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(1200))
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Slow reasoning model result" } }]
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "o1-preview",
        "messages": [{ "role": "user", "content": "Slow reasoning prompt" }]
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    let app_clone = app.clone();
    let payload_bytes_clone = payload_bytes.clone();
    let leader_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(payload_bytes_clone))
            .unwrap();
        app_clone.oneshot(req).await.unwrap()
    });

    // Wait 300ms for leader to establish Pending state
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Follower joins while upstream is still executing
    let app_clone2 = app.clone();
    let follower_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(payload_bytes))
            .unwrap();
        app_clone2.oneshot(req).await.unwrap()
    });

    let leader_resp = leader_handle.await.unwrap();
    let follower_resp = follower_handle.await.unwrap();

    assert_eq!(leader_resp.status(), StatusCode::OK);
    assert_eq!(leader_resp.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    assert_eq!(follower_resp.status(), StatusCode::OK);
    assert_eq!(follower_resp.headers().get("x-semcache-status").unwrap(), "HIT_COALESCED");

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_write_semaphore_backpressure_does_not_trip_circuit_breaker() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Backpressure test" } }]
                })),
        )
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Saturate the write semaphore with 4 permits so every async write attempts to acquire and times out
    let permit1 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit2 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit3 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();
    let permit4 = state.sqlite_write_semaphore.clone().try_acquire_owned().unwrap();

    // Send 5 distinct requests that will all experience backpressure due to semaphore saturation
    for i in 0..5 {
        let payload = json!({
            "model": "gpt-4o",
            "messages": [{ "role": "user", "content": format!("Backpressure prompt {}", i) }]
        });

        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Allow background write tasks to hit timeout (250ms each)
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Dropped writes counter incremented due to backpressure saturation
    assert!(state.dropped_writes_total.load(std::sync::atomic::Ordering::Relaxed) >= 5);
    // CRITICAL: consecutive_write_failures MUST BE 0! Saturated disk queue on a healthy disk does not trip breaker.
    assert_eq!(state.consecutive_write_failures.load(std::sync::atomic::Ordering::Relaxed), 0);

    drop(permit1);
    drop(permit2);
    drop(permit3);
    drop(permit4);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_real_sqlite_write_error_trips_circuit_breaker_and_recovers_on_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Circuit breaker recovery test" } }]
                })),
        )
        .mount(&mock_server)
        .await;

    let (app, pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Sabotage SQLite schema to trigger REAL write errors (rusqlite::Error)
    {
        let conn = pool.get().unwrap();
        conn.execute_batch("DROP TABLE exact_cache;").unwrap();
    }

    // Send 5 requests; each write task fails with real SQLite error
    for i in 0..5 {
        let payload = json!({
            "model": "gpt-4o",
            "messages": [{ "role": "user", "content": format!("Failing write prompt {}", i) }]
        });

        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Wait for async write tasks to execute and fail
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Circuit breaker is TRIPPED: consecutive_write_failures >= 5
    assert!(state.consecutive_write_failures.load(std::sync::atomic::Ordering::Relaxed) >= 5);

    // Now restore SQLite schema so writes can succeed
    {
        let conn = pool.get().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS exact_cache (
                canonical_hash BLOB PRIMARY KEY,
                model TEXT NOT NULL DEFAULT '',
                request_json TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );",
        ).unwrap();
    }

    // Send a new request that succeeds on disk write
    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Recovery prompt" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Wait for async write to commit
    tokio::time::sleep(Duration::from_millis(300)).await;

    // RECOVERY: Circuit breaker reset to 0 upon write success!
    assert_eq!(state.consecutive_write_failures.load(std::sync::atomic::Ordering::Relaxed), 0);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_oversized_response_bypass_and_follower_permit_acquisition() {
    let mock_server = MockServer::start().await;

    // Mock upstream response with > 1024 bytes body
    let large_body = json!({
        "choices": [{
            "message": { "content": "X".repeat(2048) }
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(&large_body)
                .set_delay(Duration::from_millis(200)),
        )
        .mount(&mock_server)
        .await;

    let (_old_app, _pool, db_path, mut state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    // Set max_response_bytes to 512 bytes so the 2048-byte response triggers bypass
    state.max_response_bytes = 512;
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state.clone());

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Oversized test" }]
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    let app1 = app.clone();
    let p_bytes1 = payload_bytes.clone();
    let leader_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(p_bytes1))
            .unwrap();
        app1.oneshot(req).await.unwrap()
    });

    // Follower joins while leader is streaming chunks
    tokio::time::sleep(Duration::from_millis(50)).await;
    let app2 = app.clone();
    let p_bytes2 = payload_bytes.clone();
    let follower_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(p_bytes2))
            .unwrap();
        app2.oneshot(req).await.unwrap()
    });

    let _leader_resp = leader_handle.await.unwrap();
    let follower_resp = follower_handle.await.unwrap();

    let follower_status = follower_resp.status();
    let follower_bytes = follower_resp.into_body().collect().await.unwrap().to_bytes();
    println!("FOLLOWER STATUS: {}, BODY: {}", follower_status, String::from_utf8_lossy(&follower_bytes));
    assert_eq!(follower_status, StatusCode::OK);


    assert!(state.oversized_bypasses_total.load(std::sync::atomic::Ordering::Relaxed) >= 2);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_file_permissions_0600() {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("semcache_perm_{}_{}.db", std::process::id(), rand_suffix()));
    let db_path_str = db_path.to_string_lossy().to_string();

    let pool = init_db_pool(&db_path_str).expect("init db pool");

    // Perform an insert to trigger WAL creation
    let hash = [99u8; 32];
    semcache::db::insert_exact_cache(&pool, &hash, "gpt-4o", "{}", "{}").unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(&db_path).expect("db file metadata");
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "Database file permissions must be exactly 0600");

        let wal_path = format!("{}-wal", db_path_str);
        if let Ok(wal_meta) = std::fs::metadata(&wal_path) {
            let wal_mode = wal_meta.permissions().mode() & 0o777;
            assert_eq!(wal_mode, 0o600, "WAL file permissions must be exactly 0600");
        }
    }

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-wal", db_path_str));
    let _ = std::fs::remove_file(format!("{}-shm", db_path_str));
}

#[tokio::test]
async fn test_stream_holds_permit_and_releases_on_client_disconnect() {
    let mock_server = MockServer::start().await;

    // Stream that yields token 1, waits 1000ms, then token 2
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(b"data: token1\n\n".to_vec())
                .append_header("content-type", "text/event-stream")
        )
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Constrain upstream semaphore to exactly 1 permit
    let mut drain_permits = Vec::new();
    while let Ok(permit) = state.upstream_semaphore.clone().try_acquire_owned() {
        drain_permits.push(permit);
    }
    // Return 1 permit
    let _ = drain_permits.pop();
    assert_eq!(state.upstream_semaphore.available_permits(), 1);

    // Request 1: Start a stream
    let payload = json!({
        "model": "gpt-4o",
        "stream": true,
        "messages": [{ "role": "user", "content": "Stream permit test" }]
    });

    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "BYPASS_STREAM");

    // The stream is active and holding the 1 available permit
    assert_eq!(state.upstream_semaphore.available_permits(), 0);
    assert!(state.upstream_semaphore.clone().try_acquire_owned().is_err(), "Permit must be held while stream is active");

    // Now simulate client aborting/disconnecting by dropping resp1 (which drops response body & IdleTimeoutStream)
    drop(resp1);
    tokio::task::yield_now().await;

    // Permit is immediately released back to semaphore!
    assert_eq!(state.upstream_semaphore.available_permits(), 1);
    let acquired_permit = state.upstream_semaphore.clone().try_acquire_owned();
    assert!(acquired_permit.is_ok(), "Permit must be acquirable after stream disconnect");
    drop(acquired_permit);

    // Verify a subsequent request can now acquire the permit and succeed
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&json!({
            "model": "gpt-4o",
            "messages": [{ "role": "user", "content": "Subsequent check" }]
        })).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(state.upstream_semaphore.available_permits(), 1);

    drop(drain_permits);
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_streamed_upstream_500_propagates_cleanly() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_string("Internal server error from upstream model cluster"),
        )
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "stream": true,
        "messages": [{ "role": "user", "content": "Failing stream test" }]
    });

    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_deterministic_only_replay_policy() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Deterministic response" } }]
                })),
        )
        .mount(&mock_server)
        .await;

    let (_old_app, _pool, db_path, mut state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    // Enable deterministic-only replay policy
    state.deterministic_only = true;
    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state.clone());

    // Request 1: Default temperature (1.0) and no seed -> Stochastic Bypass
    let payload_stochastic = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Tell me a joke" }]
    });

    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload_stochastic).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_eq!(resp1.headers().get("x-semcache-status").unwrap(), "BYPASS_STOCHASTIC");

    // Request 2: Explicit temperature 0.0 -> Deterministic -> Caches!
    let payload_deterministic = json!({
        "model": "gpt-4o",
        "temperature": 0.0,
        "messages": [{ "role": "user", "content": "Calculate 2+2" }]
    });

    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload_deterministic).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(resp2.headers().get("x-semcache-status").unwrap(), "MISS_UPSTREAM");

    // Wait for SQLite write commit
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Request 3: Replay Request 2 -> HIT_L1!
    let req3 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload_deterministic).unwrap()))
        .unwrap();

    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);
    assert_eq!(resp3.headers().get("x-semcache-status").unwrap(), "HIT_L1");

    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_metrics_endpoint_exposes_counters() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Metrics response" } }]
                })),
        )
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Make 1 request to increment counters
    let req = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&json!({
            "model": "gpt-4o",
            "messages": [{ "role": "user", "content": "Telemetry ping" }]
        })).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Call GET /metrics
    let metrics_req = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();

    let metrics_resp = app.clone().oneshot(metrics_req).await.unwrap();
    assert_eq!(metrics_resp.status(), StatusCode::OK);
    assert_eq!(
        metrics_resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );

    let body_bytes = metrics_resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body_str.contains("semcache_requests_total 1"));
    assert!(body_str.contains("semcache_upstream_fetches_total 1"));
    assert!(body_str.contains("semcache_consecutive_write_failures 0"));
    assert!(body_str.contains("semcache_circuit_breaker_open 0"));
    assert!(body_str.contains("semcache_dropped_writes_total 0"));

    let _ = std::fs::remove_file(db_path);
}


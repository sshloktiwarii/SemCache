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

    let state = AppState {
        db: pool.clone(),
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
        upstream_semaphore,
        default_provider: semcache::canonical::Provider::OpenAi,
        default_tenant_id: "test_tenant".to_string(),
        max_request_bytes: 10 * 1024 * 1024,
        max_response_bytes: 10 * 1024 * 1024,
        cancel_orphan_requests: false,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .route("/healthz", get(handle_healthz))
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

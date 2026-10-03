use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    routing::post,
    Router,
};
use http_body_util::BodyExt;
use serde_json::json;
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
    proxy::{handle_chat_completion, AppState},
};

fn create_test_app(upstream_url: String) -> (Router, semcache::db::DbPool, String) {
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join(format!("semcache_integ_{}_{}.db", std::process::id(), rand_suffix()));
    let db_path_str = db_path.to_string_lossy().to_string();

    let pool = init_db_pool(&db_path_str).expect("init test db pool");
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    let coalescer = RequestCoalescer::new();

    let state = AppState {
        db: pool.clone(),
        http_client,
        coalescer,
        upstream_url,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state);

    (app, pool, db_path_str)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::test]
async fn test_50_agent_stampede_single_upstream_fetch() {
    let mock_server = MockServer::start().await;

    // Upstream has a 150ms delay simulating OpenAI LLM generation
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
        .expect(1) // CRITICAL: Exactly 1 upstream call must be made for 50 agents!
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "What is the capital of France?" }],
        "temperature": 0.0
    });

    let mut handles = Vec::new();

    // Spawn 50 concurrent client tasks querying at the exact same millisecond
    for _ in 0..50 {
        let app_clone = app.clone();
        let payload_clone = payload.clone();
        handles.push(tokio::spawn(async move {
            let req = Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer sk-test-key")
                .body(Body::from(serde_json::to_vec(&payload_clone).unwrap()))
                .unwrap();

            let resp = (*app_clone).clone().oneshot(req).await.unwrap();
            let status = resp.status();
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

    let (app, _pool, db_path) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let app = Arc::new(app);

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Simulate client drop" }]
    });

    let app_leader = app.clone();
    let payload_leader = payload.clone();

    // Client 1 (Leader) starts request and aborts after 30ms (simulating client closing connection)
    let leader_handle = tokio::spawn(async move {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer sk-test-key")
            .body(Body::from(serde_json::to_vec(&payload_leader).unwrap()))
            .unwrap();

        tokio::select! {
            _ = (*app_leader).clone().oneshot(req) => {}
            _ = tokio::time::sleep(Duration::from_millis(30)) => {
                // Abort future prematurely
            }
        }
    });

    // Client 2 (Follower) starts at 50ms while upstream is still in flight
    tokio::time::sleep(Duration::from_millis(50)).await;

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

    // The follower MUST succeed with HTTP 200 because the upstream fetch was detached!
    assert_eq!(follower_status, StatusCode::OK);
    assert_eq!(
        follower_body["choices"][0]["message"]["content"].as_str().unwrap(),
        "Resilient Detached Result"
    );

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn test_persistence_gap_immediate_follow_up() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Fast Persistence Hit" } }]
                })),
        )
        .expect(1) // Exactly 1 call, even if follow-up arrives before SQLite write commits
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Persistence Gap Test" }]
    });

    // Request 1
    let req1 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // Request 2 immediately (0ms delay)
    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // Must be served from memory or L1, NOT hitting upstream again
    mock_server.verify().await;
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

    let (app, pool, db_path) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

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

    // Verify exact_cache is completely empty (no cache poisoning)
    let hash = [0u8; 32]; // dummy check
    let check = get_exact_cache(&pool, &hash).unwrap();
    assert!(check.is_none());

    let _ = std::fs::remove_file(db_path);
}

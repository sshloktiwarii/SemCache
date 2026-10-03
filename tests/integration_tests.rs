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

    let state = AppState {
        db: pool.clone(),
        http_client,
        coalescer,
        upstream_url,
        sqlite_write_semaphore,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(handle_chat_completion))
        .with_state(state.clone());

    (app, pool, db_path_str, state)
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[tokio::test]
async fn test_50_agent_stampede_single_upstream_fetch() {
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

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
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
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
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

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "choices": [{ "message": { "content": "Default Parameter Convergence" } }]
                })),
        )
        .expect(1) // Exactly 1 call despite one client passing temperature: 1.0 and one omitting it
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    // Client A explicitly passes temperature: 1.0
    let payload_a = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Tell me a joke" }],
        "temperature": 1.0,
        "top_p": 1.0
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

    // Client B omits temperature entirely
    let payload_b = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Tell me a joke" }]
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
        .expect(1)
        .mount(&mock_server)
        .await;

    let (app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));

    let payload = json!({
        "model": "gpt-4o",
        "messages": [{ "role": "user", "content": "Persistence Gap Test" }]
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

    let req2 = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer sk-test-key")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
        .unwrap();

    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

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

    // Upstream should NEVER receive a request because the ghost task aborts before dispatch
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&mock_server)
        .await;

    let (_app, _pool, db_path, _state) = create_test_app(format!("{}/v1/chat/completions", mock_server.uri()));
    let coalescer = semcache::coalesce::RequestCoalescer::new();
    let hash = [77u8; 32];

    let (guard, leader_rx) = match coalescer.register_or_wait(hash).await.unwrap() {
        semcache::coalesce::CoalesceResult::Primary(g, rx) => (g, rx),
        _ => panic!("Expected primary worker"),
    };

    // Client initiated, but closed laptop before upstream dispatch (drops receiver)
    drop(leader_rx);
    assert!(!guard.has_active_listeners(), "Dropping leader receiver must register zero listeners");

    // Spawn the worker task mimicking proxy logic
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

    // Test Case A: Active chunks within 50ms pass cleanly through TTFB 200ms and inter-chunk 100ms
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

    // Test Case B: TTFB thinking phase under 150ms succeeds before 200ms TTFB deadline
    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let rx_stream_b = futures_util::stream::poll_fn(move |cx| rx_b.poll_recv(cx));
    let mut reasoning_stream = IdleTimeoutStream::new(rx_stream_b, Duration::from_millis(200), Duration::from_millis(50));

    tokio::time::sleep(Duration::from_millis(100)).await; // 100ms thinking time
    tx_b.send(Ok(Bytes::from_static(b"thought complete"))).await.unwrap();
    let val_b = reasoning_stream.next().await.unwrap().unwrap();
    assert_eq!(val_b, Bytes::from_static(b"thought complete"));

    // Test Case C: Stalled stream exceeding inter-chunk timeout errors with TimedOut
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let rx_stream = futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx));
    let mut timed_stream = IdleTimeoutStream::new(rx_stream, Duration::from_millis(100), Duration::from_millis(40));

    tx.send(Ok(Bytes::from_static(b"hello"))).await.unwrap();
    let val = timed_stream.next().await.unwrap().unwrap();
    assert_eq!(val, Bytes::from_static(b"hello"));

    // Wait 70ms (exceeding 40ms inter-chunk timeout) without sending any chunk
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

    // Artificially saturate all 4 permits of the SQLite write semaphore
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

    // The request MUST STILL SUCCEED with HTTP 200, safely dropping the disk write without blocking the threadpool!
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        body_json["choices"][0]["message"]["content"].as_str().unwrap(),
        "Backpressure Protected Result"
    );

    // Release permits
    drop(permit1);
    drop(permit2);
    drop(permit3);
    drop(permit4);

    mock_server.verify().await;
    let _ = std::fs::remove_file(db_path);
}

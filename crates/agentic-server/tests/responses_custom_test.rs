//! Constructed regression fixtures for the custom HTTP/stream failure contract.
#[allow(dead_code)]
mod common;

use agentic_core::executor::{ConversationHandler, ExecuteRequest, ExecutionContext, ExecutorError, ResponseHandler};
use agentic_core::storage::{ConversationStore, ResponseStore, create_pool_with_schema};
use agentic_core::tool::WebSearchHandler;
use axum::{
    Router,
    body::{Body, Bytes},
    http::StatusCode,
    response::Response,
    routing::{get, post},
};
use either::Either;
use futures::StreamExt;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
impl Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl Logs {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

async fn gateway_state(upstream: &str) -> agentic_server::app::AppState {
    let mut state = common::test_state(&common::test_config(upstream));
    let pool = create_pool_with_schema(Some("sqlite::memory:")).await.unwrap();
    state.exec_ctx = Arc::new(ExecutionContext::new(
        ConversationHandler::new(ConversationStore::new(Arc::clone(&pool))),
        ResponseHandler::new(ResponseStore::new(pool)),
        Arc::new(reqwest::Client::new()),
        upstream.to_owned(),
    ));
    state
}

async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, task)
}

fn sse(events: &[Value]) -> String {
    use std::fmt::Write as _;
    events.iter().fold(String::new(), |mut body, event| {
        write!(body, "data: {event}\n\n").unwrap();
        body
    })
}
fn created() -> Value {
    json!({"type":"response.created","response":{"id":"resp_upstream","status":"in_progress"}})
}
fn terminal(kind: &str, status: &str) -> Value {
    json!({"type":kind,"response":{"id":"resp_upstream","status":status,"output":[],
        "error":{"message":"maximum context length exceeded"},"incomplete_details":{"reason":"max_output_tokens"}}})
}
fn streaming_body(body: String) -> Response {
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}
async fn request(client: &reqwest::Client, gateway: &str, store: bool) -> reqwest::Response {
    client
        .post(format!("{gateway}/v1/responses"))
        .json(
            &json!({"model":"test-model","input":"secret prompt must not appear in logs","store":store,"stream":true}),
        )
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn http_status_stream_integrity_and_operational_logs() {
    let logs = Logs::default();
    let writer = logs.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();
    let client = reqwest::Client::new();

    initial_errors(&logs, &client).await;
    body_protocol_errors(&logs, &client).await;
    terminal_errors(&logs, &client).await;
    passthrough_errors(&logs, &client).await;
    second_round_error(&logs, &client).await;
}

async fn initial_errors(logs: &Logs, client: &reqwest::Client) {
    // Initial upstream failures remain real HTTP errors and preserve retry metadata.
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::NOT_FOUND,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let app = Router::new().route(
            "/v1/responses",
            post(move || async move {
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .header("retry-after", "7")
                    .header("x-request-id", "upstream-reject")
                    .body(Body::from(r#"{"error":{"message":"maximum context length exceeded"}}"#))
                    .unwrap()
            }),
        );
        let (upstream, up_task) = serve(app).await;
        let state = gateway_state(&upstream).await;
        let (gateway, gw_task) = common::spawn_gateway(state).await;
        let response = request(client, &gateway, true).await;
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["retry-after"], "7");
        assert_eq!(response.headers()["x-request-id"], "upstream-reject");
        assert!(
            !response.headers()["content-type"]
                .to_str()
                .unwrap()
                .contains("event-stream")
        );
        let text = response.text().await.unwrap();
        assert!(text.contains("maximum context length"));
        assert!(logs.text().contains("request_error_before_stream"));
        assert!(logs.text().contains("inference_round=0"));
        assert!(!logs.text().contains("secret prompt"));
        gw_task.abort();
        up_task.abort();
        logs.clear();
    }
}

async fn body_protocol_errors(logs: &Logs, client: &reqwest::Client) {
    // Body/protocol failures after accepted status are in-band and always logged.
    for body in [
        sse(&[created()]),
        format!("{}data: [DONE]\n\n", sse(&[created()])),
        format!("{}data: {{broken\n\n", sse(&[created()])),
        sse(&[
            created(),
            json!({"type":"error","error":{"message":"maximum context length exceeded"}}),
        ]),
    ] {
        let wire = body.clone();
        let (upstream, up_task) = serve(Router::new().route(
            "/v1/responses",
            post(move || {
                let wire = wire.clone();
                async move { streaming_body(wire) }
            }),
        ))
        .await;
        let (gateway, gw_task) = common::spawn_gateway(gateway_state(&upstream).await).await;
        let response = request(client, &gateway, true).await;
        assert_eq!(response.status(), StatusCode::OK);
        let text = response.text().await.unwrap();
        assert!(text.contains("event: error") || text.contains("\"type\":\"error\""));
        assert!(!text.contains("event: response.completed"));
        let output = logs.text();
        assert!(output.contains("downstream_stream_error"));
        assert!(output.contains("response_id=resp_"));
        assert!(output.contains("model=\"test-model\"") || output.contains("model=test-model"));
        assert!(!output.contains("secret prompt"));
        gw_task.abort();
        up_task.abort();
        logs.clear();
    }
}

async fn terminal_errors(logs: &Logs, client: &reqwest::Client) {
    // Explicit failed/incomplete responses are logged even though no Rust error is raised.
    for (kind, status) in [("response.failed", "failed"), ("response.incomplete", "incomplete")] {
        let wire = sse(&[created(), terminal(kind, status)]);
        let (upstream, up_task) = serve(Router::new().route(
            "/v1/responses",
            post(move || {
                let wire = wire.clone();
                async move { streaming_body(wire) }
            }),
        ))
        .await;
        let (gateway, gw_task) = common::spawn_gateway(gateway_state(&upstream).await).await;
        let text = request(client, &gateway, true).await.text().await.unwrap();
        assert!(text.contains(kind), "{kind}: {text}\n{}", logs.text());
        assert!(logs.text().contains("downstream_terminal_failure"));
        gw_task.abort();
        up_task.abort();
        logs.clear();
    }
}

async fn passthrough_errors(logs: &Logs, client: &reqwest::Client) {
    // Stateless pass-through errors are observable without rewriting any bytes.
    let wire = sse(&[
        created(),
        json!({"type":"error","error":{"message":"maximum context length exceeded"}}),
    ]);
    let expected = wire.clone();
    let (upstream, up_task) = serve(Router::new().route(
        "/v1/responses",
        post(move || {
            let wire = wire.clone();
            async move { streaming_body(wire) }
        }),
    ))
    .await;
    let (gateway, gw_task) = common::spawn_gateway(gateway_state(&upstream).await).await;
    assert_eq!(request(client, &gateway, false).await.text().await.unwrap(), expected);
    assert!(logs.text().contains("proxy_stream_error_event"));
    assert!(logs.text().contains("context_length_exceeded"));
    gw_task.abort();
    up_task.abort();
    logs.clear();
}

async fn second_round_error(logs: &Logs, client: &reqwest::Client) {
    // Real second inference: round 0 requests a gateway tool, round 1 rejects context.
    let rounds = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let captured = Arc::clone(&rounds);
    let call = json!({"id":"fc_1","type":"function_call","call_id":"call_1","name":"web_search",
        "arguments":"{\"query\":\"test\"}","status":"completed"});
    let wire = sse(&[
        created(),
        json!({"type":"response.output_item.done","output_index":0,"item":call}),
        json!({"type":"response.completed","response":{"id":"resp_upstream","status":"completed","output":[call]}}),
    ]);
    let (upstream, up_task) = serve(Router::new().route(
        "/v1/responses",
        post(move || {
            let round = captured.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let wire = wire.clone();
            async move {
                if round == 0 {
                    streaming_body(wire)
                } else {
                    Response::builder()
                        .status(400)
                        .header("x-request-id", "round-one")
                        .body(Body::from(r#"{"error":{"message":"maximum context length exceeded"}}"#))
                        .unwrap()
                }
            }
        }),
    ))
    .await;
    let (search_url, search_task) = serve(Router::new().route(
        "/v1/search",
        get(|| async { axum::Json(json!({"results":{"web":[]},"metadata":{}})) }),
    ))
    .await;
    let mut state = gateway_state(&upstream).await;
    Arc::get_mut(&mut state.exec_ctx)
        .unwrap()
        .gateway_executors
        .insert(Arc::new(WebSearchHandler::with_api_key(
            Arc::new(reqwest::Client::new()),
            "test-key".into(),
            &search_url,
        )));
    let (gateway, gw_task) = common::spawn_gateway(state).await;
    let response = client
        .post(format!("{gateway}/v1/responses"))
        .json(
            &json!({"model":"test-model","input":"search","store":true,"stream":true,"tools":[{"type":"web_search"}]}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    assert!(text.contains("400 Bad Request"));
    assert!(text.contains("maximum context length"));
    assert_eq!(rounds.load(std::sync::atomic::Ordering::SeqCst), 2);
    let output = logs.text();
    assert!(output.contains("downstream_stream_error"));
    assert!(output.contains("inference_round=1"));
    assert!(output.contains("upstream_status=400"));
    assert!(output.contains("context_length_exceeded"));
    gw_task.abort();
    up_task.abort();
    search_task.abort();
}

#[tokio::test]
async fn initial_acceptance_waits_for_headers_and_not_first_body_chunk() {
    let (tx, rx) = oneshot::channel::<()>();
    let receiver = Arc::new(Mutex::new(Some(rx)));
    let (upstream, up_task) = serve(Router::new().route(
        "/v1/responses",
        post(move || {
            let rx = receiver.lock().unwrap().take().unwrap();
            async move {
                let body = futures::stream::once(async move {
                    let _ = rx.await;
                    Ok::<_, Infallible>(Bytes::from(sse(&[
                        created(),
                        terminal("response.completed", "completed"),
                    ])))
                });
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(body))
                    .unwrap()
            }
        }),
    ))
    .await;
    let state = gateway_state(&upstream).await;
    let payload =
        serde_json::from_value(json!({"model":"test-model","input":"hello","store":false,"stream":true})).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ExecuteRequest::new(payload, state.exec_ctx).run(),
    )
    .await
    .expect("headers release executor before TTFT")
    .unwrap();
    let Either::Right(mut stream) = result else {
        panic!("stream expected");
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    tx.send(()).unwrap();
    let text = stream.collect::<Vec<_>>().await.join("");
    assert!(text.contains("response.completed"));
    up_task.abort();
}

#[tokio::test]
async fn unsupported_local_tool_fails_before_any_stream_is_exposed() {
    let state = common::test_state(&common::test_config("http://127.0.0.1:1"));
    let payload = serde_json::from_value(json!({"model":"test-model","input":"hello","store":false,"stream":true,
        "tools":[{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}]})).unwrap();
    let result = ExecuteRequest::new(payload, state.exec_ctx).run().await;
    let Err(error) = result else {
        panic!("unsupported grammar must not expose a stream");
    };
    assert!(matches!(error, ExecutorError::Tool(_)));
    assert_eq!(error.http_status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn explicit_terminal_completes_even_if_upstream_keeps_connection_open() {
    let (upstream, up_task) = serve(Router::new().route(
        "/v1/responses",
        post(|| async {
            let body = futures::stream::once(async {
                Ok::<_, Infallible>(Bytes::from(sse(&[
                    created(),
                    terminal("response.completed", "completed"),
                ])))
            })
            .chain(futures::stream::pending());
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(body))
                .unwrap()
        }),
    ))
    .await;
    let state = common::test_state(&common::test_config(&upstream));
    let payload =
        serde_json::from_value(json!({"model":"test-model","input":"hello","store":false,"stream":true})).unwrap();
    let result = ExecuteRequest::new(payload, state.exec_ctx).run().await.unwrap();
    let Either::Right(stream) = result else {
        panic!("stream expected");
    };
    let events = tokio::time::timeout(Duration::from_secs(1), stream.collect::<Vec<_>>())
        .await
        .expect("explicit terminal must not wait for EOF, DONE, or chunk timeout");
    assert!(events.join("").contains("response.completed"));
    up_task.abort();
}

use axum::body::Body;
use axum::http::HeaderMap;
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use http::StatusCode;
use serde::de::DeserializeOwned;
use tracing::warn;

use agentic_core::executor::{BoxStream, ExecutorError};
use agentic_core::proxy::{ProxyAuth, ProxyBody, ProxyResponse, error_response_for_auth};
use agentic_core::types::request_response::RequestPayload;

pub(super) const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// # Panics
/// Panics if the response builder produces an invalid response (unreachable in practice).
pub fn convert_response(resp: ProxyResponse) -> Response {
    let mut builder = Response::builder().status(resp.status);
    for (name, value) in &resp.headers {
        builder = builder.header(name, value);
    }
    match resp.body {
        ProxyBody::Full(bytes) => builder.body(Body::from(bytes)).expect("valid response"),
        ProxyBody::Stream(stream) => builder.body(Body::from_stream(stream)).expect("valid response"),
    }
}

/// # Panics
/// Panics if the response builder produces an invalid response (unreachable in practice).
pub fn executor_error_response(err: ExecutorError) -> Response {
    let status = err.http_status();
    let error_type = err.error_type();
    let error_code = err.error_code();
    let upstream_headers = match &err {
        ExecutorError::LLMRequest { headers, .. } => Some(headers.clone()),
        _ => None,
    };

    warn!(
        phase = "request_error_response",
        status = %status,
        error_type,
        error_code,
        upstream_error = upstream_headers.is_some(),
        "returning request-level HTTP error before downstream streaming"
    );

    let mut builder = Response::builder().status(status);
    if let Some(headers) = &upstream_headers {
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
    }
    if upstream_headers
        .as_ref()
        .is_none_or(|headers| !headers.contains_key(http::header::CONTENT_TYPE))
    {
        builder = builder.header(http::header::CONTENT_TYPE, "application/json");
    }

    builder
        .body(Body::from(err.into_response_body()))
        .expect("valid error response")
}

#[allow(clippy::result_large_err)]
pub(super) async fn read_bytes(body: Body) -> Result<Bytes, Response> {
    read_bytes_with_auth(body, ProxyAuth::OpenAiBearer).await
}

#[allow(clippy::result_large_err)]
pub(super) async fn read_bytes_with_auth(body: Body, auth: ProxyAuth) -> Result<Bytes, Response> {
    axum::body::to_bytes(body, MAX_BODY_SIZE).await.map_err(|_| {
        convert_response(error_response_for_auth(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            "request body too large",
            auth,
        ))
    })
}

#[allow(clippy::result_large_err)]
pub(super) async fn read_and_parse(body: Body) -> Result<(Bytes, RequestPayload), Response> {
    let bytes = read_bytes(body).await?;
    let payload = serde_json::from_slice::<RequestPayload>(&bytes)
        .map_err(|e| executor_error_response(ExecutorError::from(e)))?;
    Ok((bytes, payload))
}

#[allow(clippy::result_large_err)]
pub(super) async fn read_json<T: DeserializeOwned>(body: Body) -> Result<T, Response> {
    let bytes = read_bytes(body).await?;
    serde_json::from_slice::<T>(&bytes).map_err(|error| executor_error_response(ExecutorError::from(error)))
}

pub(super) fn extract_store(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|j| j.get("store").and_then(serde_json::Value::as_bool))
        .unwrap_or(true)
}

pub(super) fn extract_bearer(headers: &HeaderMap, config_key: Option<&str>) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| config_key.filter(|s| !s.is_empty()).map(str::to_string))
}

pub(super) fn sse_response(stream: BoxStream) -> Response {
    sse_response_with_headers(stream, HeaderMap::new())
}

pub(super) fn sse_response_with_headers(stream: BoxStream, mut headers: HeaderMap) -> Response {
    tracing::debug!(
        phase = "downstream_sse_response_created",
        "building downstream HTTP 200 SSE response after stream readiness"
    );
    let byte_stream = stream.map(|line| Ok::<Bytes, std::convert::Infallible>(Bytes::from(line)));
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream; charset=utf-8"),
    );
    headers.insert(http::header::CACHE_CONTROL, http::HeaderValue::from_static("no-cache"));
    headers.insert("x-accel-buffering", http::HeaderValue::from_static("no"));
    let mut builder = Response::builder().status(StatusCode::OK);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from_stream(byte_stream))
        .expect("valid SSE response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn llm_request_error_preserves_retry_and_request_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("7"));
        headers.insert("x-request-id", http::HeaderValue::from_static("req_test"));
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/problem+json"),
        );
        let response = executor_error_response(ExecutorError::LLMRequest {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"error":"busy"}"#.to_owned(),
            headers,
        });

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(http::header::RETRY_AFTER).unwrap(), "7");
        assert_eq!(response.headers().get("x-request-id").unwrap(), "req_test");
        assert_eq!(
            response.headers().get(http::header::CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("read error response body");
        assert_eq!(body, Bytes::from_static(br#"{"error":"busy"}"#));
    }

    #[tokio::test]
    async fn local_executor_error_keeps_json_envelope_content_type() {
        let response = executor_error_response(ExecutorError::InvalidRequest("bad input".to_owned()));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }
}

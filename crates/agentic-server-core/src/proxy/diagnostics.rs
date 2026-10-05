//! Observe Responses SSE failures without altering pass-through bytes or status.
use crate::events::{SSEEventType, normalize_sse_line};
use crate::executor::diagnostics::{bounded_detail, error_details_enabled, error_message, reason_category};
use bytes::Bytes;
use futures::{Stream, StreamExt};

const MAX_OBSERVED_LINE_BYTES: usize = 64 * 1024;

pub(super) struct ResponseStreamObserver {
    request_id: String,
    response_id: Option<String>,
    model: String,
    status: u16,
    line: Vec<u8>,
    skipping: bool,
    terminal: bool,
    recognized: bool,
}

impl ResponseStreamObserver {
    pub(super) fn new(model: String, status: u16) -> Self {
        Self {
            request_id: crate::utils::uuid7_str("req_"),
            response_id: None,
            model,
            status,
            line: Vec::new(),
            skipping: false,
            terminal: false,
            recognized: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) {
        for part in chunk.split_inclusive(|b| *b == b'\n') {
            let ends_line = part.last() == Some(&b'\n');
            if !self.skipping && self.line.len() + part.len() > MAX_OBSERVED_LINE_BYTES {
                self.line.clear();
                self.skipping = true;
                self.log("proxy_stream_observation_limit", "diagnostic_line_too_large", None);
            }
            if !self.skipping {
                self.line.extend_from_slice(part);
            }
            if ends_line {
                if !self.skipping {
                    self.finish_line();
                }
                self.line.clear();
                self.skipping = false;
            }
        }
    }

    fn finish_line(&mut self) {
        let Ok(line) = std::str::from_utf8(&self.line) else {
            return;
        };
        let Some(frame) = normalize_sse_line(line.trim_end_matches(['\r', '\n'])) else {
            return;
        };
        if let Some(response) = frame.wire.rest.get("response") {
            if let Some(id) = response.get("id").and_then(serde_json::Value::as_str) {
                self.response_id = Some(bounded_detail(id));
            }
        }
        match frame.event_type {
            SSEEventType::ResponseCreated | SSEEventType::ResponseInProgress => self.recognized = true,
            SSEEventType::ResponseCompleted => {
                self.recognized = true;
                self.terminal = true;
            }
            SSEEventType::ResponseFailed | SSEEventType::ResponseIncomplete => {
                self.recognized = true;
                self.terminal = true;
                let detail = frame
                    .wire
                    .rest
                    .get("response")
                    .map(|r| {
                        r.get("error")
                            .and_then(error_message)
                            .or_else(|| r.get("incomplete_details").and_then(error_message))
                            .map(bounded_detail)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                self.log("proxy_stream_terminal_failure", reason_category(&detail), Some(&detail));
            }
            _ if frame.wire.event_type.as_deref() == Some("error") => {
                self.recognized = true;
                self.terminal = true;
                let detail = frame
                    .wire
                    .rest
                    .get("error")
                    .or_else(|| frame.wire.rest.get("message"))
                    .and_then(error_message)
                    .map(bounded_detail)
                    .unwrap_or_default();
                self.log("proxy_stream_error_event", reason_category(&detail), Some(&detail));
            }
            _ => {}
        }
    }

    fn finish(&mut self) {
        if !self.line.is_empty() && !self.skipping {
            self.finish_line();
        }
        if self.recognized && !self.terminal {
            self.log("proxy_stream_missing_terminal", "missing_terminal_event", None);
        }
    }

    fn log(&self, phase: &str, reason_category: &str, details: Option<&str>) {
        let error_detail = error_details_enabled().then(|| bounded_detail(details.unwrap_or_default()));
        tracing::warn!(target: "agentic_server", phase, request_id = %self.request_id,
            response_id = self.response_id.as_deref(), model = %self.model,
            downstream_http_status = self.status, reason_category, error_detail,
            "pass-through Responses stream failure observed");
    }
}

pub(super) fn observe_response_stream(
    stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    mut observer: ResponseStreamObserver,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    async_stream::stream! {
        futures::pin_mut!(stream);
        while let Some(result) = stream.next().await {
            match &result {
                Ok(bytes) => observer.observe(bytes),
                Err(_) => observer.log("proxy_stream_read_error", "network_error", None),
            }
            let failed = result.is_err();
            yield result;
            if failed { return; }
        }
        observer.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn observer_preserves_fragmented_stream_bytes() {
        let chunks = vec![
            Bytes::from_static(b"data: {\"type\":\"response.cre"),
            Bytes::from_static(b"ated\",\"response\":{\"id\":\"resp_one\"}}\n\n"),
            Bytes::from_static(b"data: {\"type\":\"error\",\"error\":{\"message\":\"maximum context length\"}}\n\n"),
        ];
        let source = futures::stream::iter(chunks.clone().into_iter().map(Ok));
        let output: Vec<_> = observe_response_stream(source, ResponseStreamObserver::new("test".into(), 200))
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(output, chunks);
    }
    #[test]
    fn oversized_lines_do_not_grow_observer_or_hide_later_error() {
        let mut observer = ResponseStreamObserver::new("test".into(), 200);
        observer.observe(&vec![b'x'; MAX_OBSERVED_LINE_BYTES + 1]);
        assert!(observer.line.is_empty());
        observer.observe(b"\ndata: {\"type\":\"error\"}\n");
        assert!(observer.terminal);
    }
}

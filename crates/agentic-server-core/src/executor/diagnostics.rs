//! Bounded, payload-free operational diagnostics for Responses failures.
use super::error::ExecutorError;

pub(crate) fn error_message(value: &serde_json::Value) -> Option<&str> {
    value
        .as_str()
        .or_else(|| value.get("message").and_then(serde_json::Value::as_str))
        .or_else(|| value.get("reason").and_then(serde_json::Value::as_str))
}

pub(crate) fn reason_category(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("context_length_exceeded")
        || lower.contains("maximum context length")
        || lower.contains("max context length")
        || lower.contains("context window")
    {
        "context_length_exceeded"
    } else if lower.contains("max_output_tokens") {
        "max_output_tokens"
    } else if lower.contains("gateway tool execution exceeded") {
        "gateway_round_limit"
    } else if lower.contains("without a terminal event") {
        "missing_terminal_event"
    } else if lower.contains("malformed data frame") {
        "malformed_stream"
    } else if lower.contains("upstream responses error event") {
        "upstream_error_event"
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "timeout"
    } else {
        "other"
    }
}

pub(crate) fn bounded_detail(message: &str) -> String {
    let mut detail = String::new();
    for ch in message.chars() {
        let ch = if ch.is_control() { ' ' } else { ch };
        if detail.len() + ch.len_utf8() > 1024 {
            break;
        }
        detail.push(ch);
    }
    detail
}

pub(crate) fn error_details_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("AGENTIC_LOG_ERROR_DETAILS").is_ok_and(|v| v == "1" || v == "true"))
}

pub(crate) fn log_error(response_id: &str, model: &str, phase: &str, round: Option<usize>, error: &ExecutorError) {
    let (upstream_status, error_bytes, upstream_request_id) = match error {
        ExecutorError::LLMRequest { status, body, headers } => (
            Some(status.as_u16()),
            body.len(),
            headers
                .get("x-request-id")
                .and_then(|id| id.to_str().ok())
                .map(bounded_detail),
        ),
        _ => (None, 0, None),
    };
    let message = error.error_message();
    let error_detail = error_details_enabled().then(|| bounded_detail(&message));
    tracing::warn!(target: "agentic_server", phase, response_id, model,
        inference_round = round, upstream_status, upstream_request_id, error_bytes,
        error_type = error.error_type(), error_code = error.error_code(),
        reason_category = reason_category(&message), error_detail,
        "Responses request failed");
}

pub(crate) fn log_terminal(response_id: &str, model: &str, status: &str, round: Option<usize>, details: Option<&str>) {
    let reason = details.map_or("unspecified", reason_category);
    if matches!(status, "failed" | "error" | "incomplete") {
        let error_detail = error_details_enabled().then(|| bounded_detail(details.unwrap_or_default()));
        tracing::warn!(target: "agentic_server", phase = "downstream_terminal_failure", response_id, model,
            inference_round = round, terminal_status = status, reason_category = reason, error_detail,
            "Responses stream returned a non-success terminal event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn context_overflow_is_classified_without_logging_payload() {
        assert_eq!(
            reason_category(
                "LLM request failed (400 Bad Request): This model's maximum context length is 262144 tokens"
            ),
            "context_length_exceeded"
        );
        assert_eq!(
            reason_category("upstream stream ended without a terminal event"),
            "missing_terminal_event"
        );
    }
    #[test]
    fn details_are_utf8_bounded_and_single_line() {
        let detail = bounded_detail(&format!("hello\n\r\t{}", "한".repeat(1024)));
        assert!(detail.len() <= 1024);
        assert!(!detail.chars().any(char::is_control));
    }
}

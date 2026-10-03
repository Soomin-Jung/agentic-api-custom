//! Live-stream integrity shares normalization and the existing accumulator transitions.
use super::identity::invalid_stream;
use super::{EventDisposition, ResponseAccumulator, StreamLifecycle, Validation};
use crate::events::{ClassifiedSseLine, EventFrame, normalize_sse_data_checked, validate_frame};
use crate::executor::error::{ExecutorError, ExecutorResult};

impl ResponseAccumulator {
    pub(in crate::executor) fn terminal_received(&self) -> bool {
        self.stream_lifecycle == StreamLifecycle::Terminal
    }

    /// Normalize classified data once, then validate and fold under the fixed policy.
    pub(in crate::executor) fn process_line(&mut self, line: ClassifiedSseLine) -> ExecutorResult<Option<EventFrame>> {
        let ClassifiedSseLine::Data(data) = line else {
            return Ok(None);
        };
        let Some(frame) = normalize_sse_data_checked(&data).map_err(|error| invalid_stream(error.to_string()))? else {
            if self.validation == Validation::Strict || self.require_terminal {
                return Err(invalid_stream("upstream stream contains a malformed data frame"));
            }
            return Ok(None);
        };
        if self.require_terminal && frame.wire.event_type.as_deref() == Some("error") {
            let details = frame
                .wire
                .rest
                .get("error")
                .or_else(|| frame.wire.rest.get("message"))
                .and_then(super::super::diagnostics::error_message)
                .map(super::super::diagnostics::bounded_detail)
                .unwrap_or_default();
            return Err(ExecutorError::StreamError(format!(
                "upstream Responses error event: {details}"
            )));
        }
        let disposition = self.process_normalized_event(&frame)?;
        Ok(disposition.into_frame(frame))
    }

    pub(super) fn process_normalized_event(&mut self, frame: &EventFrame) -> ExecutorResult<EventDisposition> {
        let validated = match self.validation {
            Validation::Strict => {
                let validated = validate_frame(frame).map_err(|error| invalid_stream(error.to_string()))?;
                self.validate_strict_transition(frame)?;
                Some(validated)
            }
            Validation::Lenient => None,
        };
        self.capture_terminal_details_if_needed(frame)?;
        self.process_event_checked(frame, validated.as_ref())
    }

    pub(super) fn finish_live_stream(&mut self) -> ExecutorResult<()> {
        if self.require_terminal && self.stream_lifecycle != StreamLifecycle::Terminal {
            return Err(ExecutorError::StreamError(
                "upstream stream ended without a terminal event".to_owned(),
            ));
        }
        self.finish_stream()
    }
}

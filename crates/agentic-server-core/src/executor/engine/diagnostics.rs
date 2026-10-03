use crate::types::io::OutputItem;
use tracing::debug;

pub(super) fn log_custom_tool_calls(output: &[OutputItem], response_id: &str) {
    for item in output {
        if let OutputItem::CustomToolCall(call) = item {
            debug!(
                response_id,
                call_id = %call.call_id,
                name = %call.name,
                input_bytes = call.input.len(),
                "custom tool call requires client execution"
            );
        }
    }
}

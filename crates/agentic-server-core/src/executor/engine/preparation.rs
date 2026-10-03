//! Round preparation, including pre-inference compaction and diagnostic numbering.
use super::prepare_initial_reasoning_for_vllm;
use crate::executor::compaction::maybe_compact_context;
use crate::executor::error::ExecutorResult;
use crate::executor::pipeline::AgentPipeline;
use crate::executor::request::ExecutionContext;
use crate::types::io::ResponseUsage;

pub(super) async fn prepare_round(
    agent: &mut AgentPipeline,
    exec_ctx: &ExecutionContext,
    auth: Option<&str>,
    round: usize,
) -> ExecutorResult<Option<ResponseUsage>> {
    agent.inference_round = Some(round);
    let usage = maybe_compact_context(&mut agent.request, exec_ctx, auth).await?;
    prepare_initial_reasoning_for_vllm(&mut agent.request.enriched_request.input, round, usage.is_some())?;
    Ok(usage)
}

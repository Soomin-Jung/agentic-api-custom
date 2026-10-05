//! Streaming execution task lifecycle, terminal validation, and failure delivery.

use super::run_until_gateway_tools_complete;
use crate::executor::error::{ExecutorError, ExecutorResult};
use crate::executor::gateway_accumulator::{
    GatewayStreamAccumulator, STREAM_EVENT_BUFFER, StreamEvent, error_sse_chunk,
};
use crate::executor::inference::{BoxStream, DONE_MARKER};
use crate::executor::persist::persist_if_needed;
use crate::executor::request::{ExecutionContext, RequestContext};
use crate::executor::stream_commit::{InitialStreamCommitWaiter, initial_stream_commit_gate};
use crate::executor::telemetry::{ExecutionSpan, FailureCategory, InstrumentedStream};
use crate::executor::upstream::agent_pipeline_with_limits;
use crate::tool::{ToolSearchMetadata, ToolSearchState};
use crate::types::request_response::ResponsePayload;
use crate::utils::common::utcnow_str;
use async_stream::stream;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

pub(super) struct AbortOnDrop<T> {
    handle: tokio::task::JoinHandle<T>,
    cancellation: Option<CancellationToken>,
}

impl<T> AbortOnDrop<T> {
    pub(super) fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            handle,
            cancellation: None,
        }
    }
}

impl<T> std::ops::Deref for AbortOnDrop<T> {
    type Target = tokio::task::JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<T> std::ops::DerefMut for AbortOnDrop<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if !self.handle.is_finished() {
            if let Some(cancellation) = &self.cancellation {
                cancellation.cancel();
            } else {
                self.handle.abort();
            }
        }
    }
}

/// `max_stream_event_bytes` is the effective limit for one serialized client
/// event on the transport that will deliver this stream. The terminal
/// `response.completed` frame is validated against it before the response is
/// persisted or a session checkpoint is published, so a frame the transport
/// cannot deliver never leaves a stored response behind.
///
/// `execution` is the request's `agentic.execute` span guard. It is moved
/// into the stream so the span stays open, and is entered on every poll,
/// until the terminal frame has been yielded or the stream is dropped. The
/// executor task is instrumented with the same span so the stages it runs
/// are parented correctly on whichever worker thread polls them.
pub(super) async fn prepare_stream(
    ctx: RequestContext,
    tool_search_state: Option<ToolSearchState>,
    exec_ctx: Arc<ExecutionContext>,
    auth: Option<String>,
    max_stream_event_bytes: usize,
    mut execution: ExecutionSpan,
) -> ExecutorResult<BoxStream> {
    let task_span = execution.span().clone();
    let failure_context = StreamFailureContext::from(&ctx);
    let (event_tx, event_rx) = mpsc::channel(STREAM_EVENT_BUFFER);
    let exec_ctx_for_run = Arc::clone(&exec_ctx);
    let mut agent = agent_pipeline_with_limits(ctx, tool_search_state, Some(event_tx), max_stream_event_bytes);
    let cancellation = agent
        .request
        .enriched_request
        .multi_agent
        .as_ref()
        .is_some_and(|config| config.enabled)
        .then(|| agent.cancellation_token());
    let (gate, mut waiter) = initial_stream_commit_gate();
    agent.initial_commit_gate = Some(gate);
    let mut run_handle = AbortOnDrop::new(tokio::spawn(
        async move {
            let result =
                run_until_gateway_tools_complete(&mut agent, exec_ctx_for_run.as_ref(), auth.as_deref(), true).await;
            if let Err(error) = &result {
                let phase = if agent.initial_commit_gate.is_some() {
                    "request_error_before_stream"
                } else {
                    "downstream_stream_error"
                };
                crate::executor::diagnostics::log_error(
                    &agent.request.response_id,
                    &agent.request.enriched_request.model,
                    phase,
                    agent.inference_round,
                    error,
                );
            }
            if let Ok((payload, _)) = &result {
                crate::executor::diagnostics::log_terminal(
                    &payload.id,
                    &payload.model,
                    &payload.status,
                    agent.inference_round,
                    payload
                        .error
                        .as_ref()
                        .and_then(crate::executor::diagnostics::error_message)
                        .or_else(|| payload.incomplete_details.as_ref().and_then(|d| d.reason.as_deref())),
                );
            }
            let (ctx, stream_accumulator) = agent.into_parts();
            (
                result.map(|(payload, metadata)| (payload, ctx, metadata)),
                stream_accumulator,
            )
        }
        .instrument(task_span),
    ));
    run_handle.cancellation = cancellation;
    let error = tokio::select! {
        ready = waiter.wait_ready() => {
            if ready.is_err() { Some(precommit_error((&mut run_handle.handle).await)) } else { None }
        }
        result = &mut run_handle.handle => Some(precommit_error(result)),
    };
    if let Some(error) = error {
        execution.failed(&error);
        execution.not_delivered();
        return Err(error);
    }
    tracing::debug!(target: "agentic_server", phase = "downstream_stream_prepared",
        response_id = %failure_context.response_id, model = %failure_context.model,
        "Responses stream is ready to expose");
    Ok(stream_from_worker(
        run_handle,
        event_rx,
        exec_ctx,
        failure_context,
        waiter,
        execution,
    ))
}

type StreamRunResult = (
    ExecutorResult<(ResponsePayload, RequestContext, Option<ToolSearchMetadata>)>,
    GatewayStreamAccumulator,
);

fn precommit_error(result: Result<StreamRunResult, tokio::task::JoinError>) -> ExecutorError {
    match result {
        Ok((Err(error), _)) => error,
        Err(error) => ExecutorError::StreamError(format!("stream task failed before response commit: {error}")),
        Ok((Ok(_), _)) => ExecutorError::StreamError("stream completed before initial response readiness".to_owned()),
    }
}

fn stream_from_worker(
    mut run_handle: AbortOnDrop<StreamRunResult>,
    mut event_rx: mpsc::Receiver<StreamEvent>,
    exec_ctx: Arc<ExecutionContext>,
    failure_context: StreamFailureContext,
    mut waiter: InitialStreamCommitWaiter,
    mut execution: ExecutionSpan,
) -> BoxStream {
    let span = execution.span().clone();
    let frames: BoxStream = Box::pin(stream! {
        if let Err(error) = waiter.release() {
            execution.failed(&error);
            yield GatewayStreamAccumulator::executor_error_chunk_at(&error, 0);
            execution.delivered();
            yield DONE_MARKER.to_owned();
            return;
        }
        tracing::debug!(target: "agentic_server", phase = "downstream_stream_started",
            response_id = %failure_context.response_id, model = %failure_context.model,
            "Responses stream body is being consumed");
            let mut next_sequence_number = 0;
            loop {
                tokio::select! {
                    Some(event) = event_rx.recv() => {
                        yield consume_stream_event(event, &mut next_sequence_number);
                    }
                    result = &mut run_handle.handle => {
                        match result {
                            Err(e) => {
                                failure_context.log_error("downstream_stream_task_error", &ExecutorError::StreamError("stream worker failed".into()));
                                if e.is_panic() {
                                    execution.failed_with(FailureCategory::Panic);
                                } else {
                                    execution.cancelled();
                                }
                                for chunk in panicked_stream_chunks(&e, &mut event_rx, &mut next_sequence_number) {
                                    yield chunk;
                                }
                                execution.delivered();
                            }
                            Ok((Err(e), mut stream_accumulator)) => {
                                execution.failed(&e);
                                while let Ok(event) = event_rx.try_recv() {
                                    yield consume_stream_event(event, &mut next_sequence_number);
                                }
                                if e.is_invalid_upstream_tool_search() {
                                    let payload = failure_context.failed_payload(&e);
                                    match stream_accumulator.terminal_response_chunk(&payload) {
                                        Ok(chunk) => yield chunk,
                                        Err(serialize_error) => {
                                            yield GatewayStreamAccumulator::executor_error_chunk_at(
                                                &serialize_error,
                                                next_sequence_number,
                                            );
                                        }
                                    }
                                } else {
                                    yield GatewayStreamAccumulator::executor_error_chunk_at(&e, next_sequence_number);
                                }
                                execution.delivered();
                                yield DONE_MARKER.to_string();
                            }
                            Ok((Ok((payload, ctx, tool_search_metadata)), stream_accumulator)) => {
                                while let Ok(event) = event_rx.try_recv() {
                                    yield consume_stream_event(event, &mut next_sequence_number);
                                }
                                let terminal = Box::pin(completed_stream_chunk(
                                    payload,
                                    ctx,
                                    tool_search_metadata,
                                    &stream_accumulator,
                                    &exec_ctx,
                                    next_sequence_number,
                                    &mut execution,
                                ))
                                .await;
                                execution.delivered();
                                yield terminal;
                                yield DONE_MARKER.to_string();
                            }
                        }
                        break;
                    }
                }
            }
    });
    Box::pin(InstrumentedStream::new(frames, span))
}

/// The terminal frame for an execution that produced a response: the
/// `response.completed` event, or an error frame when it cannot be
/// serialized or persisted.
///
/// Codex may close its WebSocket as soon as it receives `response.completed`,
/// so the response is persisted before that event is exposed and a custom
/// call/output continuation cannot be cancelled by the client disconnect.
async fn completed_stream_chunk(
    payload: ResponsePayload,
    ctx: RequestContext,
    tool_search_metadata: Option<ToolSearchMetadata>,
    stream_accumulator: &GatewayStreamAccumulator,
    exec_ctx: &ExecutionContext,
    next_sequence_number: u64,
    execution: &mut ExecutionSpan,
) -> String {
    let mut terminal_accumulator = stream_accumulator.clone();
    let chunk = match terminal_accumulator.terminal_response_chunk(&payload) {
        Ok(chunk) => chunk,
        Err(e) => {
            crate::executor::diagnostics::log_error(
                &payload.id,
                &payload.model,
                "downstream_terminal_serialization_error",
                None,
                &e,
            );
            execution.failed(&e);
            return GatewayStreamAccumulator::executor_error_chunk_at(&e, next_sequence_number);
        }
    };
    let status = payload.status.clone();
    let failure_context = StreamFailureContext::from(&ctx);
    let ch = exec_ctx.conv_handler.clone();
    let rh = exec_ctx.resp_handler.clone();
    match persist_if_needed(payload, ctx, tool_search_metadata, ch, rh).await {
        Ok(()) => {
            execution.completed_with_status(&status);
            chunk
        }
        Err(e) => {
            failure_context.log_error("downstream_persistence_error", &e);
            execution.failed(&e);
            GatewayStreamAccumulator::executor_error_chunk_at(&e, next_sequence_number)
        }
    }
}

pub(super) struct StreamFailureContext {
    response_id: String,
    conversation_id: Option<String>,
    model: String,
    previous_response_id: Option<String>,
    instructions: Option<String>,
}

impl From<&RequestContext> for StreamFailureContext {
    fn from(ctx: &RequestContext) -> Self {
        Self {
            response_id: ctx.response_id.clone(),
            conversation_id: ctx.conversation_id.clone(),
            model: ctx.enriched_request.model.clone(),
            previous_response_id: ctx.original_request.previous_response_id.clone(),
            instructions: ctx.original_request.instructions.clone(),
        }
    }
}

impl StreamFailureContext {
    fn log_error(&self, phase: &str, error: &ExecutorError) {
        crate::executor::diagnostics::log_error(&self.response_id, &self.model, phase, None, error);
    }
    pub(super) fn failed_payload(&self, error: &ExecutorError) -> ResponsePayload {
        ResponsePayload {
            id: self.response_id.clone(),
            object: "response".to_owned(),
            created_at: utcnow_str(),
            model: self.model.clone(),
            status: "failed".to_owned(),
            output: Vec::new(),
            usage: None,
            incomplete_details: None,
            error: Some(serde_json::json!({
                "message": error.error_message(),
                "type": error.error_type(),
                "code": error.error_code(),
            })),
            previous_response_id: self.previous_response_id.clone(),
            conversation_id: self.conversation_id.clone(),
            instructions: self.instructions.clone(),
            tools: None,
            tool_choice: None,
        }
    }
}

pub(super) fn consume_stream_event(event: StreamEvent, next_sequence_number: &mut u64) -> String {
    *next_sequence_number = event.sequence_number.saturating_add(1);
    event.content
}

pub(super) fn stream_task_failure_chunk(error: &tokio::task::JoinError, sequence_number: u64) -> String {
    error_sse_chunk(&format!("stream task failed: {error}"), sequence_number)
}

pub(super) fn panicked_stream_chunks(
    error: &tokio::task::JoinError,
    event_rx: &mut mpsc::Receiver<StreamEvent>,
    next_sequence_number: &mut u64,
) -> Vec<String> {
    let mut chunks = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        chunks.push(consume_stream_event(event, next_sequence_number));
    }
    chunks.push(stream_task_failure_chunk(error, *next_sequence_number));
    chunks.push(DONE_MARKER.to_owned());
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::modes::{ConversationHandler, ResponseHandler};
    use crate::storage::{ConversationStore, ResponseStore};
    use tokio::sync::oneshot;

    struct NotifyDrop(Option<oneshot::Sender<()>>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn dropping_unpolled_stream_aborts_bootstrapped_worker() {
        let (mut gate, mut waiter) = initial_stream_commit_gate();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let worker = AbortOnDrop::new(tokio::spawn(async move {
            let _notify = NotifyDrop(Some(dropped_tx));
            gate.ready_and_wait().await.unwrap();
            std::future::pending::<StreamRunResult>().await
        }));
        waiter.wait_ready().await.unwrap();
        assert!(!worker.is_finished());
        let (_sender, receiver) = mpsc::channel(STREAM_EVENT_BUFFER);
        let exec_ctx = Arc::new(ExecutionContext::new(
            ConversationHandler::new(ConversationStore::disabled()),
            ResponseHandler::new(ResponseStore::disabled()),
            Arc::new(reqwest::Client::new()),
            "http://127.0.0.1:1".to_owned(),
        ));
        let failure_context = StreamFailureContext {
            response_id: "resp_cancelled".into(),
            conversation_id: None,
            model: "test-model".into(),
            previous_response_id: None,
            instructions: None,
        };
        let stream = stream_from_worker(
            worker,
            receiver,
            exec_ctx,
            failure_context,
            waiter,
            ExecutionSpan::start(
                crate::executor::telemetry::Api::Responses,
                crate::executor::telemetry::Route::Executor,
                true,
            ),
        );
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("unpolled stream drop must abort the worker")
            .unwrap();
    }
}

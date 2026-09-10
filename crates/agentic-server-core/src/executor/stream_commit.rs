//! Coordinates the boundary between initial upstream acceptance and downstream SSE commitment.
//!
//! A streaming executor worker must not allow its caller to commit HTTP 200 until the
//! first upstream request has returned a successful HTTP status. The worker signals
//! readiness after that status is known, then waits for the caller to release it before
//! consuming the upstream response body.

use tokio::sync::oneshot;

use crate::executor::error::{ExecutorError, ExecutorResult};

pub(super) struct InitialStreamCommitGate {
    ready_tx: Option<oneshot::Sender<()>>,
    release_rx: Option<oneshot::Receiver<()>>,
}

pub(super) struct InitialStreamCommitWaiter {
    ready_rx: oneshot::Receiver<()>,
    release_tx: Option<oneshot::Sender<()>>,
}

pub(super) fn initial_stream_commit_gate() -> (InitialStreamCommitGate, InitialStreamCommitWaiter) {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    (
        InitialStreamCommitGate {
            ready_tx: Some(ready_tx),
            release_rx: Some(release_rx),
        },
        InitialStreamCommitWaiter {
            ready_rx,
            release_tx: Some(release_tx),
        },
    )
}

impl InitialStreamCommitGate {
    /// Mark the initial request as safe to commit downstream, then wait until the
    /// caller has observed readiness and released body processing.
    pub(super) async fn ready_and_wait(&mut self) -> ExecutorResult<()> {
        let ready_tx = self.ready_tx.take().ok_or_else(|| {
            ExecutorError::StreamError("initial stream commit gate was signalled more than once".to_owned())
        })?;
        tracing::debug!(
            target: "agentic_server",
            phase = "downstream_commit_ready",
            "initial upstream response is accepted; downstream SSE is safe to commit"
        );
        ready_tx.send(()).map_err(|()| {
            ExecutorError::StreamError("initial stream commit waiter closed before readiness".to_owned())
        })?;

        let release_rx = self.release_rx.take().ok_or_else(|| {
            ExecutorError::StreamError("initial stream commit gate release was consumed more than once".to_owned())
        })?;
        release_rx.await.map_err(|_| {
            ExecutorError::StreamError("initial stream commit waiter closed before release".to_owned())
        })?;
        tracing::debug!(
            target: "agentic_server",
            phase = "downstream_commit_released",
            "downstream caller acknowledged commit readiness; upstream body consumption may continue"
        );
        Ok(())
    }
}

impl InitialStreamCommitWaiter {
    pub(super) async fn wait_ready(&mut self) -> Result<(), oneshot::error::RecvError> {
        (&mut self.ready_rx).await
    }

    pub(super) fn release(&mut self) -> ExecutorResult<()> {
        let release_tx = self.release_tx.take().ok_or_else(|| {
            ExecutorError::StreamError("initial stream commit waiter released more than once".to_owned())
        })?;
        release_tx.send(()).map_err(|()| {
            ExecutorError::StreamError("initial stream worker closed before release".to_owned())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn commit_gate_blocks_worker_until_waiter_releases() {
        let (mut gate, mut waiter) = initial_stream_commit_gate();
        let worker = tokio::spawn(async move {
            gate.ready_and_wait().await.expect("gate succeeds");
            "released"
        });

        waiter.wait_ready().await.expect("worker announces readiness");
        assert!(!worker.is_finished(), "worker must remain blocked before release");
        waiter.release().expect("release succeeds");
        assert_eq!(worker.await.expect("worker joins"), "released");
    }
}

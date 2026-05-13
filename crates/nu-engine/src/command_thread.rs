use std::sync::{Arc, atomic::AtomicBool, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nu_protocol::{
    PipelineData, ShellError, Signals,
    ast::Block,
    debugger::WithoutDebug,
    engine::{EngineState, Stack},
};
use nu_system::SuspendState;

use crate::eval_ir::eval_ir_block;

thread_local! {
    static IS_COMMAND_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Returns `true` when called from a pipeline worker thread spawned by [`CommandThread`].
///
/// Used by `should_use_threaded_pipeline` to prevent recursive threading.
pub fn is_on_command_thread() -> bool {
    IS_COMMAND_THREAD.with(|c| c.get())
}

/// State preserved when a thread-based pipeline is frozen mid-eval.
///
/// Stored inside [`FrozenJob::pipeline_state`] as `Box<dyn Any + Send>`.
/// On `job unfreeze`, this is downcast back to `FrozenCommandThreadState` and used
/// to re-enter the orchestrator wait loop.
pub struct FrozenCommandThreadState {
    pub result_rx: mpsc::Receiver<Result<PipelineData, ShellError>>,
    pub frozen_rx: mpsc::Receiver<()>,
    pub suspend_state: Arc<SuspendState>,
    pub interrupt: Arc<AtomicBool>,
}

/// Manages a pipeline worker thread that can be cooperatively suspended (frozen) and resumed.
///
/// The worker evaluates the block with suspend-aware `Signals`, then sends the entire
/// `Result<PipelineData, ShellError>` through a rendezvous channel. The worker thread
/// exits after sending.
///
/// Freeze/resume during eval (Phase 1) is handled by the orchestrator in `eval_block_threaded`.
/// Freeze/resume during streaming (Phase 2) is handled by `SuspendableIter` after the result
/// is returned to the main thread.
pub struct CommandThread {
    /// Rendezvous channel carrying the worker's result.
    pub result_rx: mpsc::Receiver<Result<PipelineData, ShellError>>,
    /// Receives `()` when the worker thread parks on the suspend condvar.
    pub frozen_rx: mpsc::Receiver<()>,
    pub suspend_state: Arc<SuspendState>,
    pub interrupt: Arc<AtomicBool>,
    // Kept alive so the thread is joined on drop; dropped when CommandThread drops.
    _join_handle: JoinHandle<()>,
}

impl CommandThread {
    /// Spawns a pipeline worker thread that evaluates `block` with `input`.
    ///
    /// The spawned thread shares the caller's interrupt `Arc<AtomicBool>` so Ctrl+C propagates.
    /// A fresh `SuspendState` enables cooperative Ctrl+Z suspension.
    pub fn spawn(
        engine_state: &EngineState,
        stack: Stack,
        block: Arc<Block>,
        input: PipelineData,
    ) -> Self {
        let suspend_state = Arc::new(SuspendState::new());
        let (frozen_tx, frozen_rx) = mpsc::sync_channel::<()>(1);
        suspend_state.set_frozen_notifier(frozen_tx);

        // Share the main interrupt so Ctrl+C propagates into the pipeline thread.
        let interrupt = engine_state
            .signals()
            .interrupt_arc()
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

        let signals = Signals::with_suspend(interrupt.clone(), suspend_state.clone());

        let mut worker_engine_state = engine_state.clone();
        worker_engine_state.set_signals(signals);

        // Rendezvous channel (capacity 0): worker blocks on send until orchestrator recvs.
        let (result_tx, result_rx) = mpsc::sync_channel::<Result<PipelineData, ShellError>>(0);

        let join_handle = thread::Builder::new()
            .name("pipeline-worker".into())
            .spawn(move || {
                IS_COMMAND_THREAD.with(|c| c.set(true));

                let result = eval_ir_block::<WithoutDebug>(
                    &worker_engine_state,
                    &mut { stack },
                    &block,
                    input,
                );

                let result: Result<PipelineData, ShellError> = match result {
                    Err(ShellError::Exit { code }) => std::process::exit(code),
                    Ok(ped) => Ok(ped.body),
                    Err(e) => Err(e),
                };

                let _ = result_tx.send(result);
            })
            .expect("failed to spawn pipeline worker thread");

        CommandThread {
            result_rx,
            frozen_rx,
            suspend_state,
            interrupt,
            _join_handle: join_handle,
        }
    }

    /// Ask the pipeline thread to cooperatively park at the next yield point.
    pub fn suspend(&self) {
        self.suspend_state.suspend();
    }

    /// Wait up to `timeout` for the pipeline thread to confirm it is parked.
    ///
    /// Returns `true` if the thread confirmed it is parked within the timeout.
    pub fn wait_for_frozen(&self, timeout: Duration) -> bool {
        self.frozen_rx.recv_timeout(timeout).is_ok()
    }

    /// Consume this `CommandThread` into a [`FrozenCommandThreadState`].
    ///
    /// The worker thread is detached but will exit naturally when the rendezvous
    /// channel receiver is dropped (causing `send` to fail).
    pub fn into_frozen_state(self) -> FrozenCommandThreadState {
        FrozenCommandThreadState {
            result_rx: self.result_rx,
            frozen_rx: self.frozen_rx,
            suspend_state: self.suspend_state,
            interrupt: self.interrupt,
        }
    }

    /// Reconstruct a `CommandThread` from a previously frozen state.
    ///
    /// Used by `job unfreeze` to re-enter the orchestrator wait loop after resuming
    /// the parked worker thread.
    pub fn from_frozen(state: FrozenCommandThreadState) -> Self {
        // The worker thread is still alive (parked on the condvar). We don't have a
        // JoinHandle because it was dropped when `into_frozen_state` was called, so
        // use `thread::spawn` of a no-op to satisfy the type. The worker manages itself.
        let dummy_handle = thread::Builder::new()
            .name("pipeline-worker-dummy".into())
            .spawn(|| {})
            .expect("failed to spawn dummy thread");

        CommandThread {
            result_rx: state.result_rx,
            frozen_rx: state.frozen_rx,
            suspend_state: state.suspend_state,
            interrupt: state.interrupt,
            _join_handle: dummy_handle,
        }
    }
}

use std::sync::{Arc, atomic::AtomicBool, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nu_protocol::{
    ShellError, Signals,
    engine::{EngineState, Stack},
};
use nu_system::SuspendState;

thread_local! {
    static IS_COMMAND_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Returns `true` when called from a pipeline worker thread spawned by [`CommandThread`].
///
/// Prevents recursive threading: closures produced by lazy commands like `each` run on the
/// worker thread during streaming, and should not spawn further worker threads.
pub fn is_on_command_thread() -> bool {
    IS_COMMAND_THREAD.with(|c| c.get())
}

/// State preserved when a thread-based pipeline is frozen mid-execution.
///
/// Stored inside [`FrozenJob::pipeline_state`] as `Box<dyn Any + Send>`.
/// On `job unfreeze`, this is downcast back to [`FrozenCommandThreadState`] and used
/// to reconstruct a [`CommandThread`] that re-enters the orchestrator wait loop.
pub struct FrozenCommandThreadState {
    pub result_rx: mpsc::Receiver<Result<Stack, ShellError>>,
    pub frozen_rx: mpsc::Receiver<()>,
    pub suspend_state: Arc<SuspendState>,
    pub interrupt: Arc<AtomicBool>,
}

/// Manages a pipeline worker thread that can be cooperatively suspended (frozen) and resumed.
///
/// The worker runs a caller-supplied closure with a clone of the engine state that has
/// suspend-aware [`Signals`]. It returns the final [`Stack`] via a rendezvous channel so the
/// main thread can propagate stack mutations (env vars, variable assignments, etc.).
///
/// Both eval and print happen on the worker thread, so every `signals.check()` call anywhere
/// in the pipeline — including inside `sleep`, `each` closures, and consuming commands — is a
/// cooperative freeze point.
pub struct CommandThread {
    /// Rendezvous channel (capacity 0) carrying the worker's result.
    pub result_rx: mpsc::Receiver<Result<Stack, ShellError>>,
    /// Receives `()` when the worker thread parks on the suspend condvar.
    pub frozen_rx: mpsc::Receiver<()>,
    pub suspend_state: Arc<SuspendState>,
    pub interrupt: Arc<AtomicBool>,
    // Kept alive so the thread is joined on drop.
    _join_handle: JoinHandle<()>,
}

impl CommandThread {
    /// Spawn a pipeline worker thread that runs `work` with suspend-aware [`Signals`].
    ///
    /// `work` receives a `&mut EngineState` pre-configured with cooperative Ctrl+Z support.
    /// It should run eval + print and return the final [`Stack`]. The thread exits after
    /// sending its result via the rendezvous channel.
    pub fn spawn_with<F>(engine_state: &EngineState, work: F) -> Self
    where
        F: FnOnce(&mut EngineState) -> Result<Stack, ShellError> + Send + 'static,
    {
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
        let (result_tx, result_rx) = mpsc::sync_channel::<Result<Stack, ShellError>>(0);

        let join_handle = thread::Builder::new()
            .name("pipeline-worker".into())
            .spawn(move || {
                IS_COMMAND_THREAD.with(|c| c.set(true));
                let result = work(&mut worker_engine_state);
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
    /// The `_join_handle` is dropped, detaching the thread. The worker remains alive
    /// (parked on the condvar) and will send its result when resumed.
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
        // JoinHandle because it was dropped when `into_frozen_state` was called.
        // Use a no-op dummy thread to satisfy the type.
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

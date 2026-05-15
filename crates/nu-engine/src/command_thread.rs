use std::sync::{Arc, atomic::AtomicBool};
use std::thread;

use nu_protocol::{
    ShellError, Signals,
    engine::{CommandThread, EngineState, Stack},
};
use nu_system::SuspendState;

/// Spawn a pipeline worker thread that runs `work` with suspend-aware [`Signals`].
///
/// The worker receives a cloned `EngineState` pre-configured with cooperative Ctrl+Z support.
/// It should run eval + print and return the final [`Stack`].
pub fn spawn_with<F>(engine_state: &EngineState, work: F) -> CommandThread
where
    F: FnOnce(&mut EngineState) -> Result<Stack, ShellError> + Send + 'static,
{
    let suspend_state = Arc::new(SuspendState::new());

    let interrupt = engine_state
        .signals()
        .interrupt_arc()
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    let signals = Signals::new(interrupt.clone(), Some(suspend_state.clone()));

    let mut worker_engine_state = engine_state.clone();
    worker_engine_state.set_signals(signals);
    worker_engine_state.is_command_thread = true;

    let ss = suspend_state.clone();
    let join_handle = thread::Builder::new()
        .name("pipeline-worker".into())
        .spawn(move || {
            let result = work(&mut worker_engine_state);
            ss.mark_finished();
            result
        })
        .expect("failed to spawn pipeline worker thread");

    CommandThread::new(suspend_state, interrupt, join_handle)
}

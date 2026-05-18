use std::sync::{Arc, atomic::AtomicBool};
use std::thread;

use nu_protocol::{
    ShellError, Signals,
    engine::{CommandThread, EngineState, Stack},
};
#[cfg(unix)]
use nu_system::SuspendState;

/// Spawn a pipeline worker thread that runs `work` with suspend-aware [`Signals`].
///
/// The worker receives a cloned `EngineState` pre-configured with cooperative Ctrl+Z support
/// (Unix only; on other platforms the thread runs without suspend capability).
/// It should run eval + print and return the final [`Stack`].
pub fn spawn_with<F>(engine_state: &EngineState, work: F) -> CommandThread
where
    F: FnOnce(&mut EngineState) -> Result<Stack, ShellError> + Send + 'static,
{
    #[cfg(unix)]
    let suspend_state = Arc::new(SuspendState::new());

    let interrupt = engine_state
        .signals()
        .interrupt_arc()
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

    #[cfg(unix)]
    let signals = Signals::new(interrupt.clone(), Some(suspend_state.clone()));
    #[cfg(not(unix))]
    let signals = Signals::new(interrupt.clone(), None);

    let mut worker_engine_state = engine_state.clone();
    worker_engine_state.set_signals(signals);
    worker_engine_state.is_command_thread = true;

    #[cfg(unix)]
    let ss = suspend_state.clone();
    let join_handle = thread::Builder::new()
        .name("pipeline-worker".into())
        .spawn(move || {
            let result = work(&mut worker_engine_state);
            #[cfg(unix)]
            ss.mark_finished();
            result
        })
        .expect("failed to spawn pipeline worker thread");

    CommandThread::new(
        #[cfg(unix)]
        suspend_state,
        interrupt,
        join_handle,
    )
}

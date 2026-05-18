use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::JoinHandle;

#[cfg(unix)]
use nu_system::SuspendState;

use crate::{ShellError, engine::Stack};

/// A handle to a pipeline worker thread that can be cooperatively suspended and resumed.
///
/// Created by `spawn_with` in `nu-engine`. Call [`join`](Self::join) to await the result
/// or [`detach`](Self::detach) to let the thread run independently.
pub struct CommandThread {
    #[cfg(unix)]
    pub suspend_state: Arc<SuspendState>,
    pub interrupt: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<Result<Stack, ShellError>>>,
}

impl std::fmt::Debug for CommandThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandThread").finish_non_exhaustive()
    }
}

impl CommandThread {
    pub fn new(
        #[cfg(unix)] suspend_state: Arc<SuspendState>,
        interrupt: Arc<AtomicBool>,
        join_handle: JoinHandle<Result<Stack, ShellError>>,
    ) -> Self {
        CommandThread {
            #[cfg(unix)]
            suspend_state,
            interrupt,
            join_handle: Some(join_handle),
        }
    }

    /// Ask the worker to cooperatively park at its next yield point.
    #[cfg(unix)]
    pub fn suspend(&self) {
        self.suspend_state.suspend();
    }

    /// Wait for the worker thread to finish and return its result.
    pub fn join(mut self) -> Result<Stack, ShellError> {
        self.join_handle
            .take()
            .expect("join called twice")
            .join()
            .unwrap_or_else(|_| {
                Err(ShellError::NushellFailed {
                    msg: "pipeline worker thread panicked".into(),
                })
            })
    }

    /// Drop the join handle, letting the thread run to completion without waiting.
    pub fn detach(self) {
        // Dropping self drops the JoinHandle, detaching the thread.
    }

    /// Set the interrupt flag and wake the worker if it is parked.
    pub fn kill(&self) {
        self.interrupt.store(true, Ordering::SeqCst);
        #[cfg(unix)]
        self.suspend_state.resume();
    }
}

use crate::{ShellError, Span};
use nu_glob::Interruptible;
use nu_system::SuspendState;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Inner state shared across [`Signals`] clones.
///
/// Stored in an `Arc` so that cloning `Signals` remains cheap and the interrupt/suspend
/// state is shared between all clones.
#[derive(Debug)]
struct SignalsInner {
    interrupt: Arc<AtomicBool>,
    suspend: Option<Arc<SuspendState>>,
}

/// Used to check for signals to suspend or terminate the execution of Nushell code.
///
/// Supports both interruption (ctrl+c or SIGINT) and cooperative suspension for internal
/// pipelines (ctrl+z / SIGTSTP via [`SuspendState`]).
#[derive(Debug, Clone)]
pub struct Signals {
    inner: Option<Arc<SignalsInner>>,
}

impl Signals {
    /// A [`Signals`] that is not hooked up to any event/signals source.
    ///
    /// So, this [`Signals`] will never be interrupted.
    pub const EMPTY: Self = Signals { inner: None };

    /// Create a new [`Signals`] with `ctrlc` as the interrupt source.
    ///
    /// Once `ctrlc` is set to `true`, [`check`](Self::check) will error
    /// and [`interrupted`](Self::interrupted) will return `true`.
    ///
    /// Pass `Some(suspend)` to enable cooperative suspension for pipeline worker threads.
    pub fn new(ctrlc: Arc<AtomicBool>, suspend: Option<Arc<SuspendState>>) -> Self {
        Self {
            inner: Some(Arc::new(SignalsInner {
                interrupt: ctrlc,
                suspend,
            })),
        }
    }

    /// Create a [`Signals`] that is not hooked up to any event/signals source.
    ///
    /// So, the returned [`Signals`] will never be interrupted.
    ///
    /// This should only be used in test code, or if the stream/iterator being created
    /// already has an underlying [`Signals`].
    pub const fn empty() -> Self {
        Self::EMPTY
    }

    /// Returns an `Err` if an interrupt has been triggered.
    ///
    /// Also cooperatively parks the calling thread if a suspend has been requested,
    /// before checking for interrupts.
    ///
    /// Otherwise, returns `Ok`.
    #[inline]
    pub fn check(&self, span: &Span) -> Result<(), ShellError> {
        #[inline]
        #[cold]
        fn interrupt_error(span: &Span) -> Result<(), ShellError> {
            Err(ShellError::Interrupted { span: *span })
        }

        self.wait_if_suspended();
        if self.interrupted() {
            interrupt_error(span)
        } else {
            Ok(())
        }
    }

    /// Triggers an interrupt.
    pub fn trigger(&self) {
        if let Some(inner) = &self.inner {
            inner.interrupt.store(true, Ordering::Relaxed);
        }
    }

    /// Returns whether an interrupt has been triggered.
    #[inline]
    pub fn interrupted(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|i| i.interrupt.load(Ordering::Relaxed))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_none()
    }

    pub fn reset(&self) {
        if let Some(inner) = &self.inner {
            inner.interrupt.store(false, Ordering::Relaxed);
        }
    }

    /// Cooperative yield point. Blocks the calling thread if a suspend has been requested;
    /// returns immediately otherwise. No-op if this `Signals` has no suspend state.
    ///
    /// On Unix, also checks `SIGTSTP_FLAG` directly so that command-thread workers can
    /// self-suspend at yield points without waiting for the orchestrator's polling interval.
    #[inline]
    pub fn wait_if_suspended(&self) {
        if let Some(inner) = &self.inner
            && let Some(s) = &inner.suspend
        {
            #[cfg(unix)]
            if nu_system::SIGTSTP_FLAG.load(std::sync::atomic::Ordering::SeqCst) {
                s.suspend();
            }

            s.wait_if_suspended();
        }
    }

    /// Returns the shared interrupt `Arc<AtomicBool>`, if any.
    ///
    /// Used by `CommandThread` to share the interrupt flag between the main thread
    /// and the pipeline worker thread.
    pub fn interrupt_arc(&self) -> Option<Arc<AtomicBool>> {
        self.inner.as_ref().map(|i| i.interrupt.clone())
    }

    /// Request the thread to park at its next yield point.
    ///
    /// No-op if this `Signals` has no suspend state (e.g., background jobs or the REPL).
    pub fn suspend(&self) {
        if let Some(inner) = &self.inner
            && let Some(s) = &inner.suspend
        {
            s.suspend();
        }
    }

    /// Clear any pending suspension request so the thread will not park at the next yield point.
    ///
    /// Used after a nested orchestrator returns to ensure that a suspension flag set by an outer
    /// orchestrator does not unexpectedly park this thread later.
    ///
    /// No-op if this `Signals` has no suspend state.
    pub fn resume(&self) {
        if let Some(inner) = &self.inner
            && let Some(s) = &inner.suspend
        {
            s.resume();
        }
    }
}

impl Interruptible for Signals {
    #[inline]
    fn interrupted(&self) -> bool {
        self.interrupted()
    }
}

/// The types of things that can be signaled. It's anticipated this will change as we learn more
/// about how we'd like signals to be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalAction {
    Interrupt,
    Reset,
}

use std::sync::{Arc, Mutex, atomic::Ordering};

use nu_protocol::{
    PipelineData, PipelineMetadata, Signals, Span, ValueIterator,
    engine::{EngineState, FrozenJob, Job, Jobs},
};
use nu_system::{SIGTSTP_FLAG, UnfreezeHandle};

/// Iterator state preserved across a freeze/resume cycle.
///
/// Stored in [`FrozenJob::pipeline_state`] as `Box<dyn Any + Send>`.
/// On `job unfreeze`, downcast back to `FrozenIteratorState` to reconstitute the stream.
pub struct FrozenIteratorState {
    pub inner: ValueIterator,
    pub span: Span,
    /// Metadata carries `row_offset` — the number of rows already displayed before this freeze.
    pub metadata: Option<PipelineMetadata>,
}

/// A thin wrapper iterator that checks `SIGTSTP_FLAG` before each value pull.
///
/// When Ctrl+Z is detected, the remaining inner iterator is stored as a [`FrozenJob`]
/// and `None` is returned to end the current print loop. On `job unfreeze`, the stored
/// iterator is reconstituted into a new `ListStream` for resumed printing.
///
/// Tracks `items_consumed` so that on freeze the metadata `row_offset` is updated correctly,
/// allowing resumed output to continue row numbering seamlessly.
pub struct SuspendableIter {
    inner: Option<ValueIterator>,
    jobs: Arc<Mutex<Jobs>>,
    is_interactive: bool,
    span: Span,
    metadata: Option<PipelineMetadata>,
    items_consumed: usize,
}

impl SuspendableIter {
    pub fn new(
        inner: ValueIterator,
        jobs: Arc<Mutex<Jobs>>,
        is_interactive: bool,
        span: Span,
        metadata: Option<PipelineMetadata>,
    ) -> Self {
        SuspendableIter {
            inner: Some(inner),
            jobs,
            is_interactive,
            span,
            metadata,
            items_consumed: 0,
        }
    }
}

impl Iterator for SuspendableIter {
    type Item = nu_protocol::Value;

    fn next(&mut self) -> Option<Self::Item> {
        // Check for Ctrl+Z (SIGTSTP) before pulling the next value.
        if SIGTSTP_FLAG.swap(false, Ordering::SeqCst) {
            let inner = self.inner.take()?;

            // Carry the row offset forward so resumed output continues numbering correctly.
            // The base row_offset (from a previous freeze) plus items consumed in this run
            // gives the total rows already displayed. Always create metadata even if the
            // original stream had none, so the offset is not lost on unfreeze.
            let mut frozen_metadata = self.metadata.clone().unwrap_or_default();
            frozen_metadata.row_offset += self.items_consumed;

            let frozen_state = FrozenIteratorState {
                inner,
                span: self.span,
                metadata: Some(frozen_metadata),
            };

            let job = Job::Frozen(FrozenJob {
                unfreeze: UnfreezeHandle::Iterator,
                description: Some("pipeline".into()),
                pipeline_state: Some(Box::new(frozen_state)),
            });

            let job_id = self.jobs.lock().expect("jobs lock").add_job(job);

            if self.is_interactive {
                eprintln!("\nJob {} is frozen", job_id.get());
            }

            return None;
        }

        let value = self.inner.as_mut()?.next();
        if value.is_some() {
            self.items_consumed += 1;
        }
        value
    }
}

/// Wrap the pipeline's `ListStream` in a [`SuspendableIter`] at the REPL pull boundary.
///
/// Clears any stale `SIGTSTP_FLAG` from a previous pipeline, then wraps the output.
/// Only wraps when running interactively and not in a background job. All other
/// `PipelineData` variants are returned unchanged (ByteStream, Value, Empty).
#[cfg(unix)]
pub fn wrap_suspendable(engine_state: &EngineState, data: PipelineData) -> PipelineData {
    use nu_protocol::ListStream;

    // Clear any stale SIGTSTP from a previous pipeline.
    SIGTSTP_FLAG.store(false, Ordering::SeqCst);

    if !engine_state.is_interactive || engine_state.is_background_job() {
        return data;
    }

    match data {
        PipelineData::ListStream(stream, metadata) => {
            let span = stream.span();
            let inner = stream.into_inner();
            let suspendable = SuspendableIter::new(
                inner,
                engine_state.jobs.clone(),
                engine_state.is_interactive,
                span,
                metadata.clone(),
            );
            let new_stream = ListStream::new(suspendable, span, Signals::empty());
            PipelineData::list_stream(new_stream, metadata)
        }
        other => other,
    }
}

#[cfg(not(unix))]
pub fn wrap_suspendable(_engine_state: &EngineState, data: PipelineData) -> PipelineData {
    data
}

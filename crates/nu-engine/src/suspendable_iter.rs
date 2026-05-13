use std::sync::{Arc, Mutex, atomic::Ordering};

#[cfg(unix)]
use nu_protocol::Signals;
use nu_protocol::{
    FrozenIteratorState, PipelineData, PipelineMetadata, Span, ValueIterator,
    engine::{EngineState, FrozenJob, Job, Jobs},
};
use nu_system::{SIGTSTP_FLAG, UnfreezeHandle};

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
    metadata: PipelineMetadata,
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
            metadata: metadata.unwrap_or_default(),
        }
    }
}

impl Iterator for SuspendableIter {
    type Item = nu_protocol::Value;

    fn next(&mut self) -> Option<Self::Item> {
        // Check for Ctrl+Z (SIGTSTP) before pulling the next value.
        if SIGTSTP_FLAG.swap(false, Ordering::SeqCst) {
            let inner = self.inner.take()?;

            let frozen_state = FrozenIteratorState {
                inner,
                span: self.span,
                metadata: self.metadata.clone(),
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
            self.metadata.row_offset += 1;
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

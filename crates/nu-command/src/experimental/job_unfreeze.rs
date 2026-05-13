use nu_engine::command_prelude::*;
use nu_protocol::{
    JobId,
    engine::{FrozenJob, Job, ThreadJob},
    process::check_ok,
};
use nu_system::{ForegroundWaitStatus, UnfreezeHandle, kill_by_pid};

#[derive(Clone)]
pub struct JobUnfreeze;

impl Command for JobUnfreeze {
    fn name(&self) -> &str {
        "job unfreeze"
    }

    fn description(&self) -> &str {
        "Unfreeze a frozen process job in foreground."
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::build("job unfreeze")
            .category(Category::Experimental)
            .optional("id", SyntaxShape::Int, "The process id to unfreeze.")
            .input_output_types(vec![(Type::Nothing, Type::Any)])
            .allow_variants_without_examples(true)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["fg"]
    }

    fn run(
        &self,
        engine_state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let head = call.head;

        let mut jobs = engine_state.jobs.lock().expect("jobs lock is poisoned!");

        let id: Option<usize> = call.opt(engine_state, stack, 0)?;
        let id = id
            .map(JobId::new)
            .or_else(|| jobs.most_recent_frozen_job_id())
            .ok_or(JobError::NoneToUnfreeze { span: head })?;

        let job = match jobs.lookup(id) {
            None => return Err(JobError::NotFound { span: head, id }.into()),
            Some(Job::Thread(ThreadJob { .. })) => {
                return Err(JobError::CannotUnfreeze { span: head, id }.into());
            }
            Some(Job::Frozen(FrozenJob { .. })) => jobs
                .remove_job(id)
                .expect("job was supposed to be in job list"),
        };

        drop(jobs);

        unfreeze_job(engine_state, id, job, head)
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "job unfreeze",
                description: "Unfreeze the latest frozen job.",
                result: None,
            },
            Example {
                example: "job unfreeze 4",
                description: "Unfreeze a specific frozen job by its PID.",
                result: None,
            },
        ]
    }

    fn extra_description(&self) -> &str {
        "When a running process is frozen (with the SIGTSTP signal or with the Ctrl-Z key on unix),
a background job gets registered for this process, which can then be resumed using this command."
    }
}

fn unfreeze_job(
    state: &EngineState,
    old_id: JobId,
    job: Job,
    span: Span,
) -> Result<PipelineData, ShellError> {
    match job {
        Job::Thread(ThreadJob { .. }) => Err(JobError::CannotUnfreeze { span, id: old_id }.into()),
        Job::Frozen(FrozenJob {
            unfreeze: handle,
            description,
            pipeline_state,
        }) => {
            // Thread-based pipeline jobs: resume the worker and re-enter the wait loop.
            // The worker continues printing from where it was suspended; we just wait for it.
            #[cfg(unix)]
            if let UnfreezeHandle::Thread { .. } = &handle {
                return unfreeze_thread_job(state, pipeline_state);
            }

            // External process job.
            let pid = handle.pid();

            if pid > 0
                && let Some(thread_job) = &state.current_thread_job()
                && !thread_job.try_add_pid(pid)
            {
                kill_by_pid(pid.into()).map_err(|err| {
                    ShellError::Io(IoError::new_internal(
                        err,
                        "job was interrupted; could not kill foreground process",
                    ))
                })?;
            }

            let result = handle.unfreeze(
                state
                    .is_interactive
                    .then(|| state.pipeline_externals_state.clone()),
            );

            if pid > 0
                && let Some(thread_job) = &state.current_thread_job()
            {
                thread_job.remove_pid(pid);
            }

            match result {
                Ok(ForegroundWaitStatus::Frozen(handle)) => {
                    let mut jobs = state.jobs.lock().expect("jobs lock is poisoned!");

                    jobs.add_job_with_id(
                        old_id,
                        Job::Frozen(FrozenJob {
                            unfreeze: handle,
                            description,
                            pipeline_state: None,
                        }),
                    )
                    .expect("job was supposed to be removed");

                    if state.is_interactive {
                        println!("\nJob {} is re-frozen", old_id.get());
                    }
                    Ok(PipelineData::Empty)
                }

                Ok(ForegroundWaitStatus::Finished(status)) => {
                    check_ok(status, false, span)?;
                    Ok(PipelineData::Empty)
                }

                Err(err) => Err(ShellError::Io(IoError::new_internal(
                    err,
                    "Failed to unfreeze foreground process",
                ))),
            }
        }
    }
}

/// Resume a thread-based pipeline job by re-entering the orchestrator wait loop.
///
/// Resumes the parked worker thread, then hands off to `orchestrate_command_thread`
/// which polls for completion while checking for Ctrl+Z (re-freeze) and Ctrl+C.
/// The worker handles printing internally; this function returns `Empty` when done.
#[cfg(unix)]
fn unfreeze_thread_job(
    engine_state: &EngineState,
    pipeline_state: Option<Box<dyn std::any::Any + Send>>,
) -> Result<PipelineData, ShellError> {
    use nu_engine::{CommandThread, FrozenCommandThreadState, orchestrate_command_thread};

    let Some(frozen) = pipeline_state
        .and_then(|b| b.downcast::<FrozenCommandThreadState>().ok())
        .map(|b| *b)
    else {
        return Ok(PipelineData::Empty);
    };

    // Resume the worker — it will unpark from its condvar and continue executing.
    frozen.suspend_state.resume();

    let ct = CommandThread::from_frozen(frozen);
    // Worker may finish, re-freeze, or be interrupted. In all cases we return Empty
    // since the worker handles printing and stack mutations are discarded (the resumed
    // pipeline was from a previous REPL entry).
    orchestrate_command_thread(engine_state, ct)?;
    Ok(PipelineData::Empty)
}

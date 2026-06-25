use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use crate::counter;
use crate::processing::strategies::{
    CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use crate::types::Message;

const TOUCH_INTERVAL: Duration = Duration::from_secs(1);

pub struct HealthCheck<Next> {
    next_step: Next,
    path: PathBuf,
    interval: Duration,
    deadline: SystemTime,
    iterations_since_last_submit: u64,
}

impl<Next> HealthCheck<Next> {
    pub fn new(next_step: Next, path: impl Into<PathBuf>) -> Self {
        let interval = TOUCH_INTERVAL;
        let deadline = SystemTime::now() + interval;

        Self {
            next_step,
            path: path.into(),
            interval,
            deadline,
            iterations_since_last_submit: 0,
        }
    }

    fn maybe_touch_file(&mut self) {
        let now = SystemTime::now();
        if now < self.deadline {
            return;
        }

        if let Err(err) = std::fs::File::create(&self.path) {
            let error: &dyn std::error::Error = &err;
            tracing::error!(error);
        }

        counter!("arroyo.processing.strategies.healthcheck.touch");
        self.deadline = now + self.interval;
    }
}

impl<TPayload, Next> ProcessingStrategy<TPayload> for HealthCheck<Next>
where
    Next: ProcessingStrategy<TPayload> + 'static,
{
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        let poll_result = self.next_step.poll();
        if let Ok(Some(_commit_request)) = poll_result.as_ref() {
            self.maybe_touch_file();
        }

        if self.iterations_since_last_submit > 0 {
            self.maybe_touch_file();
        }

        self.iterations_since_last_submit += 1;
        poll_result
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), SubmitError<TPayload>> {
        self.iterations_since_last_submit = 0;
        self.next_step.submit(message)
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.next_step.join(timeout)
    }
}

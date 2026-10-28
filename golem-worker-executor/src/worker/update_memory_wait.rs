// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source License v1.1 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded waiting for memory by a snapshot-based (manual) update.
//!
//! A memory grow the executor's admission gate refuses while an update saves or
//! loads its snapshot says the executor is out of memory right now, not that the
//! update cannot work: the same snapshot loads once memory has been freed. Such a
//! step is therefore retried — the agent is unloaded, releasing its memory, and
//! restarted with the out-of-memory backoff — instead of recording the update as
//! failed. [`UpdateMemoryWait`] decides how long that may go on: from the first
//! refusal for an update until `timeout` has passed, after which the update is
//! recorded as failed after all.

use golem_common::model::component::ComponentRevision;
use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

/// The step of a snapshot-based update whose memory grow was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateSnapshotStep {
    /// Saving the snapshot in the old revision.
    Save,
    /// Loading the snapshot into the target revision.
    Load,
}

impl Display for UpdateSnapshotStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Save => write!(f, "saving the snapshot"),
            Self::Load => write!(f, "loading the snapshot"),
        }
    }
}

/// What to do about an update whose memory grow was just refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateMemoryDecision {
    /// Keep the update pending and retry the step once memory may be back.
    /// `retry` counts the retries of this update so far, this one included.
    Retry { retry: u32, waited: Duration },
    /// The update has waited for memory long enough; record it as failed.
    GiveUp { retries: u32, waited: Duration },
}

impl UpdateMemoryDecision {
    /// The failure details recorded for an update that gave up waiting.
    pub fn failure_details(step: UpdateSnapshotStep, retries: u32, waited: Duration) -> String {
        format!(
            "Manual update failed: out of memory while {step}, still refused after {retries} retries over {}s",
            waited.as_secs()
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Waiting {
    target_revision: ComponentRevision,
    since: Instant,
    retries: u32,
}

/// Tracks how long the pending snapshot-based update of one agent has been
/// waiting for memory. Lives on the agent's `Worker`, so it survives the unloads
/// and restarts of the retries.
#[derive(Debug, Default)]
pub struct UpdateMemoryWait {
    waiting: Option<Waiting>,
}

impl UpdateMemoryWait {
    /// Called when a memory grow of the update to `target_revision` was refused
    /// at `now`. The wait starts at the first refusal for that revision; a
    /// refusal for another revision starts a new wait.
    pub fn on_refused(
        &mut self,
        target_revision: ComponentRevision,
        now: Instant,
        timeout: Duration,
    ) -> UpdateMemoryDecision {
        let waiting = match self.waiting {
            Some(waiting) if waiting.target_revision == target_revision => waiting,
            _ => Waiting {
                target_revision,
                since: now,
                retries: 0,
            },
        };
        let waited = now.saturating_duration_since(waiting.since);
        if waited >= timeout {
            self.waiting = None;
            UpdateMemoryDecision::GiveUp {
                retries: waiting.retries,
                waited,
            }
        } else {
            let retry = waiting.retries + 1;
            self.waiting = Some(Waiting {
                retries: retry,
                ..waiting
            });
            UpdateMemoryDecision::Retry { retry, waited }
        }
    }

    /// The update finished (succeeded or failed): forget its wait.
    pub fn clear(&mut self) {
        self.waiting = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_r::test;

    const TIMEOUT: Duration = Duration::from_secs(30 * 60);

    fn revision(n: u64) -> ComponentRevision {
        ComponentRevision::new(n).unwrap()
    }

    #[test]
    fn first_refusal_retries() {
        let mut wait = UpdateMemoryWait::default();
        let now = Instant::now();
        assert_eq!(
            wait.on_refused(revision(2), now, TIMEOUT),
            UpdateMemoryDecision::Retry {
                retry: 1,
                waited: Duration::ZERO
            }
        );
    }

    #[test]
    fn retries_until_the_timeout_counted_from_the_first_refusal() {
        let mut wait = UpdateMemoryWait::default();
        let start = Instant::now();
        wait.on_refused(revision(2), start, TIMEOUT);
        assert_eq!(
            wait.on_refused(revision(2), start + Duration::from_secs(60), TIMEOUT),
            UpdateMemoryDecision::Retry {
                retry: 2,
                waited: Duration::from_secs(60)
            }
        );
        assert_eq!(
            wait.on_refused(revision(2), start + TIMEOUT, TIMEOUT),
            UpdateMemoryDecision::GiveUp {
                retries: 2,
                waited: TIMEOUT
            }
        );
    }

    #[test]
    fn a_refusal_for_another_revision_starts_a_new_wait() {
        let mut wait = UpdateMemoryWait::default();
        let start = Instant::now();
        wait.on_refused(revision(2), start, TIMEOUT);
        assert_eq!(
            wait.on_refused(revision(3), start + TIMEOUT, TIMEOUT),
            UpdateMemoryDecision::Retry {
                retry: 1,
                waited: Duration::ZERO
            }
        );
    }

    #[test]
    fn giving_up_or_clearing_forgets_the_wait() {
        let mut wait = UpdateMemoryWait::default();
        let start = Instant::now();
        wait.on_refused(revision(2), start, TIMEOUT);
        wait.on_refused(revision(2), start + TIMEOUT, TIMEOUT);
        assert_eq!(
            wait.on_refused(revision(2), start + TIMEOUT * 2, TIMEOUT),
            UpdateMemoryDecision::Retry {
                retry: 1,
                waited: Duration::ZERO
            }
        );

        wait.clear();
        assert_eq!(
            wait.on_refused(revision(2), start + TIMEOUT * 3, TIMEOUT),
            UpdateMemoryDecision::Retry {
                retry: 1,
                waited: Duration::ZERO
            }
        );
    }

    #[test]
    fn failure_details_say_it_ran_out_of_memory() {
        let details = UpdateMemoryDecision::failure_details(
            UpdateSnapshotStep::Load,
            7,
            Duration::from_secs(1800),
        );
        assert_eq!(
            details,
            "Manual update failed: out of memory while loading the snapshot, still refused after 7 retries over 1800s"
        );
    }
}

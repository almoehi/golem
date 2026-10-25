use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;

/// A source agent whose last processed oplog entry is older than this (measured against the
/// newest entry time of the batch being committed) is forgotten. Its open spans can no longer
/// complete in any batch this plugin will see: the agent was deleted, crashed without a
/// terminal entry, or is an ephemeral agent whose later entries are never forwarded. Keeping
/// such state forever grew the plugin's state and snapshot without bound (2026-10-25 stall).
pub(crate) const STALE_AGENT_STATE_TTL_NS: u128 = 60 * 60 * 1_000_000_000;

/// Upper bound on the source agents tracked at once; the least recently seen are forgotten
/// first. Bounds the state (and the snapshot) even under a burst of short-lived agents that
/// all stay within the TTL.
pub(crate) const MAX_TRACKED_AGENTS: usize = 2_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct WorkerState {
    pub(crate) trace_id: String,
    pub(crate) trace_states: Vec<String>,
    pub(crate) pending_spans: HashMap<String, PendingSpan>,
    pub(crate) implicit_spans: Vec<PendingSpan>,
    pub(crate) terminal_error: Option<String>,
    /// Maps inherited span_id → resolved parent span_id. For inherited local spans,
    /// this is the nearest ancestor that was not inherited. For external spans,
    /// this maps the external span id to itself so local spans can use it as a
    /// remote parent. Belongs to the invocation in flight; cleared when it ends.
    pub(crate) inherited_span_parents: HashMap<String, Option<String>>,
    /// Timestamp (nanos) when the current invocation started, for duration metrics.
    pub(crate) invocation_start_ns: Option<u128>,
    /// Running total of linear memory in bytes (initial + sum of grow deltas).
    pub(crate) total_memory_bytes: u64,
    /// Running count of active resources (created - dropped).
    pub(crate) active_resources: i64,
    /// Timestamp (nanos) of the newest oplog entry processed for this agent; the eviction
    /// clock (see `commit`). Taken from the entries, never from a host clock, so replaying
    /// the plugin worker derives the same state. 0 = unknown (state from an older snapshot).
    #[serde(default)]
    pub(crate) last_seen_ns: u128,
}

impl WorkerState {
    pub(crate) fn new() -> Self {
        Self {
            trace_id: String::new(),
            trace_states: Vec::new(),
            pending_spans: HashMap::new(),
            implicit_spans: Vec::new(),
            terminal_error: None,
            inherited_span_parents: HashMap::new(),
            invocation_start_ns: None,
            total_memory_bytes: 0,
            active_resources: 0,
            last_seen_ns: 0,
        }
    }

    /// Whether nothing must be carried to the agent's next batch: no invocation in flight
    /// and nothing it opened. The running memory / resource totals are deliberately not
    /// carried for an idle agent.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending_spans.is_empty()
            && self.implicit_spans.is_empty()
            && self.terminal_error.is_none()
            && self.inherited_span_parents.is_empty()
            && self.invocation_start_ns.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingSpan {
    pub(crate) span_id: String,
    pub(crate) parent_span_id: Option<String>,
    pub(crate) start_time_ns: u128,
    pub(crate) attributes: HashMap<String, String>,
}

thread_local! {
    /// Derived per-source-agent state (open spans, ...), keyed by `helpers::worker_key`.
    /// The only state this component keeps across `process` calls, so it is all a
    /// snapshot has to carry (see `snapshot.rs`). Bounded by `commit`.
    pub(crate) static WORKER_STATES: RefCell<HashMap<String, WorkerState>> =
        RefCell::new(HashMap::new());
}

/// The single place a processed agent's state is stored: drops it when nothing is left to
/// carry, then forgets every agent not seen for `STALE_AGENT_STATE_TTL_NS` and, beyond
/// `MAX_TRACKED_AGENTS`, the least recently seen ones.
pub(crate) fn commit(states: &mut HashMap<String, WorkerState>, key: String, state: WorkerState) {
    let now_ns = state.last_seen_ns;
    if state.is_empty() {
        states.remove(&key);
    } else {
        states.insert(key, state);
    }
    evict_stale(states, now_ns);
}

fn evict_stale(states: &mut HashMap<String, WorkerState>, now_ns: u128) {
    if now_ns > 0 {
        let before = states.len();
        states.retain(|_, state| {
            if state.last_seen_ns == 0 {
                // Restored from a snapshot that predates the eviction clock: start its TTL now.
                state.last_seen_ns = now_ns;
            }
            now_ns.saturating_sub(state.last_seen_ns) <= STALE_AGENT_STATE_TTL_NS
        });
        let evicted = before - states.len();
        if evicted > 0 {
            println!("OTLP exporter: forgot {evicted} agent(s) not seen for over an hour");
        }
    }

    let excess = states.len().saturating_sub(MAX_TRACKED_AGENTS);
    if excess > 0 {
        let mut by_age: Vec<(u128, String)> = states
            .iter()
            .map(|(key, state)| (state.last_seen_ns, key.clone()))
            .collect();
        by_age.sort_unstable();
        for (_, key) in by_age.into_iter().take(excess) {
            states.remove(&key);
        }
        println!("OTLP exporter: tracking more than {MAX_TRACKED_AGENTS} agents, forgot the {excess} least recently seen");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_NS: u128 = 1_000_000_000;

    /// An agent with an invocation in flight (never finished in any batch we see).
    fn open_state(last_seen_ns: u128) -> WorkerState {
        WorkerState {
            invocation_start_ns: Some(last_seen_ns),
            last_seen_ns,
            ..WorkerState::new()
        }
    }

    // Regression (2026-10-25 stall): ephemeral agents' final oplog entries never reach the
    // plugin, so their open invocation never ends here; such entries (2.5k of 3.6k at the
    // last good snapshot) were kept forever.
    #[test]
    fn agents_not_seen_for_longer_than_the_ttl_are_forgotten() {
        let mut states = HashMap::new();
        commit(&mut states, "stale".to_string(), open_state(SECOND_NS));
        commit(&mut states, "recent".to_string(), open_state(1_800 * SECOND_NS));

        commit(
            &mut states,
            "current".to_string(),
            open_state(SECOND_NS + STALE_AGENT_STATE_TTL_NS + SECOND_NS),
        );

        let mut keys: Vec<_> = states.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["current".to_string(), "recent".to_string()]);
    }

    #[test]
    fn tracked_agents_are_capped_by_forgetting_the_least_recently_seen() {
        let mut states = HashMap::new();
        for i in 0..(MAX_TRACKED_AGENTS + 10) {
            commit(&mut states, format!("agent-{i}"), open_state(SECOND_NS + i as u128));
        }

        assert_eq!(states.len(), MAX_TRACKED_AGENTS);
        assert!(!states.contains_key("agent-9"));
        assert!(states.contains_key("agent-10"));
    }

    #[test]
    fn state_restored_without_a_clock_starts_its_ttl_at_the_next_commit() {
        let mut states = HashMap::from([("restored".to_string(), open_state(0))]);

        commit(&mut states, "current".to_string(), open_state(5 * SECOND_NS));

        assert_eq!(states["restored"].last_seen_ns, 5 * SECOND_NS);
    }

    #[test]
    fn an_agent_with_nothing_left_to_carry_is_removed() {
        let mut states = HashMap::from([("idle".to_string(), open_state(SECOND_NS))]);

        commit(
            &mut states,
            "idle".to_string(),
            WorkerState {
                last_seen_ns: 2 * SECOND_NS,
                ..WorkerState::new()
            },
        );

        assert!(states.is_empty());
    }
}

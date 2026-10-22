//! Snapshot support: with `save-snapshot` / `load-snapshot` exported, the worker executor
//! snapshots this oplog processor periodically (`oplog.oplog_processor_snapshotting`), so
//! recovering the plugin worker loads its last snapshot and replays only the invocations
//! after it, instead of every batch it ever processed (each carrying the source oplog
//! entries of that batch as its parameters).

use crate::state::{WORKER_STATES, WorkerState};
use golem_rust::load_snapshot::exports::golem::api::load_snapshot::Guest as LoadSnapshotGuest;
use golem_rust::save_snapshot::exports::golem::api::save_snapshot::{
    Guest as SaveSnapshotGuest, Snapshot as SavedSnapshot,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const SNAPSHOT_MIME_TYPE: &str = "application/json";
const SNAPSHOT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct SnapshotV1 {
    version: u32,
    worker_states: HashMap<String, WorkerState>,
}

pub(crate) fn encode(worker_states: &HashMap<String, WorkerState>) -> Vec<u8> {
    #[derive(Serialize)]
    struct SnapshotRef<'a> {
        version: u32,
        worker_states: &'a HashMap<String, WorkerState>,
    }
    serde_json::to_vec(&SnapshotRef {
        version: SNAPSHOT_VERSION,
        worker_states,
    })
    .expect("worker states serialize to JSON")
}

pub(crate) fn decode(payload: &[u8]) -> Result<HashMap<String, WorkerState>, String> {
    let snapshot: SnapshotV1 = serde_json::from_slice(payload)
        .map_err(|e| format!("OTLP exporter: unreadable snapshot: {e}"))?;
    if snapshot.version != SNAPSHOT_VERSION {
        return Err(format!(
            "OTLP exporter: unsupported snapshot version {}",
            snapshot.version
        ));
    }
    Ok(snapshot.worker_states)
}

pub(crate) struct OtlpExporterSnapshots;

impl SaveSnapshotGuest for OtlpExporterSnapshots {
    fn save() -> SavedSnapshot {
        SavedSnapshot {
            payload: WORKER_STATES.with(|states| encode(&states.borrow())),
            mime_type: SNAPSHOT_MIME_TYPE.to_string(),
        }
    }
}

impl LoadSnapshotGuest for OtlpExporterSnapshots {
    fn load(
        snapshot: golem_rust::load_snapshot::exports::golem::api::load_snapshot::Snapshot,
    ) -> Result<(), String> {
        let worker_states = decode(&snapshot.payload)?;
        WORKER_STATES.with(|states| *states.borrow_mut() = worker_states);
        Ok(())
    }
}

golem_rust::save_snapshot::export_save_snapshot!(OtlpExporterSnapshots with_types_in golem_rust::save_snapshot);
golem_rust::load_snapshot::export_load_snapshot!(OtlpExporterSnapshots with_types_in golem_rust::load_snapshot);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PendingSpan;

    fn worker_state() -> WorkerState {
        WorkerState {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            trace_states: vec!["vendor=1".to_string()],
            pending_spans: HashMap::from([(
                "span-1".to_string(),
                PendingSpan {
                    span_id: "span-1".to_string(),
                    parent_span_id: Some("span-0".to_string()),
                    start_time_ns: u128::from(u64::MAX) * 3,
                    attributes: HashMap::from([("k".to_string(), "v".to_string())]),
                },
            )]),
            implicit_spans: Vec::new(),
            terminal_error: None,
            inherited_span_parents: HashMap::from([("ext".to_string(), None)]),
            invocation_start_ns: Some(1_700_000_000_000_000_000),
            total_memory_bytes: 65_536,
            active_resources: -1,
        }
    }

    #[test]
    fn snapshot_round_trips_worker_states() {
        let states = HashMap::from([("component/agent".to_string(), worker_state())]);

        assert_eq!(decode(&encode(&states)).unwrap(), states);
    }

    #[test]
    fn snapshot_of_unknown_version_is_refused() {
        let payload = br#"{"version":2,"worker_states":{}}"#;

        assert!(decode(payload).unwrap_err().contains("version 2"));
    }

    #[test]
    fn unreadable_snapshot_is_refused() {
        assert!(decode(b"not json").is_err());
    }
}

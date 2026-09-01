mod config;
mod export;
mod helpers;
mod otlp_json;
mod processing;
mod state;

use config::ExporterConfig;
use export::{build_resource_attributes, send_logs, send_metrics, send_spans};
use helpers::worker_key;
use otlp_json::{
    ExportLogsServiceRequest, ExportMetricsServiceRequest, ExportTraceServiceRequest,
    InstrumentationScope, OtlpResource, ResourceLogs, ResourceMetrics, ResourceSpans, ScopeLogs,
    ScopeMetrics, ScopeSpans,
};
use processing::process_entries;
use state::WORKER_STATES;

use golem_rust::bindings::golem::api::oplog::{OplogEntry, OplogIndex};
use golem_rust::golem_wasm::golem_core_1_5_x::types::{AgentId, ComponentId};
use golem_rust::oplog_processor::exports::golem::api::oplog_processor::Guest as OplogProcessorGuest;

use std::collections::HashMap;

struct OtlpExporterComponent;

impl OplogProcessorGuest for OtlpExporterComponent {
    fn process(
        _account_info: golem_rust::oplog_processor::exports::golem::api::oplog_processor::AccountInfo,
        config: Vec<(String, String)>,
        component_id: ComponentId,
        worker_id: AgentId,
        metadata: golem_rust::bindings::golem::api::host::AgentMetadata,
        _first_entry_index: OplogIndex,
        entries: Vec<OplogEntry>,
    ) -> Result<(), String> {
        if entries.is_empty() {
            return Ok(());
        }

        let exporter_config = match ExporterConfig::from_params(&config) {
            Ok(Some(c)) => c,
            Ok(None) => return Ok(()),
            Err(e) => {
                return Err(format!(
                    "OTLP exporter: configuration error: {e}"
                ));
            }
        };

        let key = worker_key(&component_id, &worker_id);

        // Clone current state for processing — do NOT mutate the stored state yet
        let mut working_state = WORKER_STATES.with(|states| {
            let states = states.borrow();
            states.get(&key).cloned().unwrap_or_else(|| state::WorkerState {
                trace_id: String::new(),
                trace_states: Vec::new(),
                pending_spans: HashMap::new(),
                implicit_spans: Vec::new(),
                terminal_error: None,
                inherited_span_parents: HashMap::new(),
                invocation_start_ns: None,
                total_memory_bytes: 0,
                active_resources: 0,
            })
        });

        let output = process_entries(&mut working_state, entries);

        // Commit the derived state unconditionally, right after processing — before any
        // fallible network I/O below. `working_state` (pending_spans, invocation_start_ns,
        // inherited_span_parents, ...) is the ONLY record of how this batch's oplog entries
        // were interpreted; the batch itself is never redelivered once handed to this
        // function (redelivery, where it happens, retries the OTLP send, not
        // re-derivation — see the is_empty() cleanup below, unchanged). If the commit were
        // deferred until after a successful export (the previous behavior), any export
        // failure — including a transient one, e.g. the collector being briefly
        // unreachable — would silently discard this batch's derived spans/timestamps,
        // permanently orphaning any StartSpan/AgentInvocationStarted whose matching
        // FinishSpan/AgentInvocationFinished arrives in a later, successful batch.
        WORKER_STATES.with(|states| {
            let mut states = states.borrow_mut();
            if working_state.is_empty() {
                states.remove(&key);
            } else {
                states.insert(key, working_state);
            }
        });

        let has_traces = exporter_config.signals.traces && !output.spans.is_empty();
        let has_logs = exporter_config.signals.logs && !output.log_records.is_empty();
        let has_metrics = exporter_config.signals.metrics && !output.metrics.is_empty();

        if !has_traces && !has_logs && !has_metrics {
            return Ok(());
        }

        let resource_attrs =
            build_resource_attributes(&exporter_config, &component_id, &worker_id, &metadata);

        let scope = InstrumentationScope {
            name: "golem-otlp-exporter".to_string(),
            version: "1.5.0".to_string(),
        };

        if has_traces {
            let span_count = output.spans.len();
            let request_body = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: OtlpResource {
                        attributes: resource_attrs.clone(),
                    },
                    scope_spans: vec![ScopeSpans {
                        scope: scope.clone(),
                        spans: output.spans,
                    }],
                }],
            };
            // Export failures are deliberately NOT propagated as `Err` from `process()` —
            // see the state-commit comment above. Returning `Err` here previously made
            // golem treat a transient export failure (e.g. collector briefly down) the
            // same as a genuine plugin crash: it can trigger the platform's oplog
            // redelivery/retry path, which — combined with this being a single shared,
            // multi-tenant worker instance — risks stalling delivery to ALL source
            // workers behind a permanently-retrying batch, not just losing this one
            // batch's telemetry. Best effort: log and move on.
            if let Err(e) = send_spans(&exporter_config, request_body) {
                eprintln!(
                    "OTLP exporter: failed to export {span_count} trace span(s), dropping this batch's traces: {e}"
                );
            } else {
                println!("OTLP: exported {span_count} trace span(s)");
            }
        }

        if has_logs {
            let log_count = output.log_records.len();
            let request_body = ExportLogsServiceRequest {
                resource_logs: vec![ResourceLogs {
                    resource: OtlpResource {
                        attributes: resource_attrs.clone(),
                    },
                    scope_logs: vec![ScopeLogs {
                        scope: scope.clone(),
                        log_records: output.log_records,
                    }],
                }],
            };
            if let Err(e) = send_logs(&exporter_config, request_body) {
                eprintln!(
                    "OTLP exporter: failed to export {log_count} log record(s), dropping this batch's logs: {e}"
                );
            } else {
                println!("OTLP: exported {log_count} log record(s)");
            }
        }

        if has_metrics {
            let metric_count = output.metrics.len();
            let request_body = ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: OtlpResource {
                        attributes: resource_attrs,
                    },
                    scope_metrics: vec![ScopeMetrics {
                        scope,
                        metrics: output.metrics,
                    }],
                }],
            };
            if let Err(e) = send_metrics(&exporter_config, request_body) {
                eprintln!(
                    "OTLP exporter: failed to export {metric_count} metric(s), dropping this batch's metrics: {e}"
                );
            } else {
                println!("OTLP: exported {metric_count} metric(s)");
            }
        }

        Ok(())
    }
}

golem_rust::oplog_processor::export_oplog_processor!(OtlpExporterComponent with_types_in golem_rust::oplog_processor);

use crate::counters::CounterAccumulator;
use crate::export::build_otel_span;
use crate::helpers::{
    attribute_value_to_string, datetime_to_nanos, oplog_payload_size, timestamp_to_nanos,
    worker_error_to_string, wrapped_function_type_name,
};
use crate::otlp_json::{
    KeyValue, OtlpGauge, OtlpLogRecord, OtlpMetric, OtlpNumberDataPoint, OtlpSpan, OtlpSum,
    OtlpValue,
};
use crate::state::{PendingSpan, WorkerState};
use golem_rust::bindings::golem::api::oplog::{
    FailedUpdateParameters, FinishSpanParameters, GrowMemoryParameters, LogLevel, LogParameters,
    OplogEntry, OplogPayload, RawAgentInvocationFinishedParameters,
    RawAgentInvocationStartedParameters, RawCreateParameters, RawCreateResourceParameters,
    RawDropResourceParameters, RawHostCallParameters, RawOplogProcessorCheckpointParameters,
    RawSnapshotParameters, RawSuccessfulUpdateParameters, RemoteTransactionParameters,
    SetSpanAttributeParameters, SpanData, StartSpanParameters, WrappedFunctionType,
};
use std::collections::{HashMap, HashSet};

pub(crate) struct ProcessingOutput {
    pub(crate) spans: Vec<OtlpSpan>,
    pub(crate) log_records: Vec<OtlpLogRecord>,
    pub(crate) metrics: Vec<OtlpMetric>,
}

pub(crate) fn process_entries(
    state: &mut WorkerState,
    entries: Vec<OplogEntry>,
) -> ProcessingOutput {
    let mut completed_spans: Vec<OtlpSpan> = Vec::new();
    let mut log_records: Vec<OtlpLogRecord> = Vec::new();
    let mut metrics: Vec<OtlpMetric> = Vec::new();
    let mut counters = CounterAccumulator::default();

    for entry in entries {
        match entry {
            OplogEntry::Create(params) => {
                handle_create(state, params, &mut metrics);
            }
            OplogEntry::AgentInvocationStarted(params) => {
                handle_invocation_started(state, params, &mut counters);
            }
            OplogEntry::StartSpan(params) => {
                handle_start_span(state, params);
            }
            OplogEntry::SetSpanAttribute(params) => {
                handle_set_span_attribute(state, params);
            }
            OplogEntry::FinishSpan(params) => {
                handle_finish_span(state, params, &mut completed_spans);
            }
            OplogEntry::AgentInvocationFinished(params) => {
                handle_invocation_finished(state, params, &mut completed_spans, &mut metrics);
            }
            OplogEntry::Error(params) => {
                let error_msg = worker_error_to_string(&params.error);
                state.terminal_error = Some(error_msg.clone());

                let time_ns = datetime_to_nanos(&params.timestamp);

                handle_terminal(state, time_ns, true, &mut completed_spans);

                counters.add(
                    "golem.error.count",
                    "1",
                    "Agent errors",
                    time_ns,
                    1,
                    vec![KeyValue {
                        key: "error.type".to_string(),
                        value: OtlpValue {
                            string_value: worker_error_variant_name(&params.error),
                        },
                    }],
                );

                log_records.push(OtlpLogRecord {
                    time_unix_nano: time_ns.to_string(),
                    observed_time_unix_nano: time_ns.to_string(),
                    severity_number: 17,
                    severity_text: "ERROR".to_string(),
                    body: Some(OtlpValue {
                        string_value: error_msg,
                    }),
                    attributes: vec![KeyValue {
                        key: "error.type".to_string(),
                        value: OtlpValue {
                            string_value: worker_error_variant_name(&params.error),
                        },
                    }],
                    trace_id: non_empty_trace_id(&state.trace_id),
                    span_id: None,
                });
            }
            OplogEntry::Interrupted(ts) => {
                let time_ns = timestamp_to_nanos(&ts);
                state.terminal_error = Some("interrupted".to_string());
                handle_terminal(state, time_ns, true, &mut completed_spans);

                counters.add(
                    "golem.interruption.count",
                    "1",
                    "Agent interruptions",
                    time_ns,
                    1,
                    Vec::new(),
                );

                log_records.push(OtlpLogRecord {
                    time_unix_nano: time_ns.to_string(),
                    observed_time_unix_nano: time_ns.to_string(),
                    severity_number: 13,
                    severity_text: "WARN".to_string(),
                    body: Some(OtlpValue {
                        string_value: "Agent interrupted".to_string(),
                    }),
                    attributes: Vec::new(),
                    trace_id: non_empty_trace_id(&state.trace_id),
                    span_id: None,
                });
            }
            OplogEntry::Exited(ts) => {
                let time_ns = timestamp_to_nanos(&ts);
                state.terminal_error = Some("exited".to_string());
                handle_terminal(state, time_ns, true, &mut completed_spans);

                counters.add(
                    "golem.exit.count",
                    "1",
                    "Agent exits",
                    time_ns,
                    1,
                    Vec::new(),
                );

                log_records.push(OtlpLogRecord {
                    time_unix_nano: time_ns.to_string(),
                    observed_time_unix_nano: time_ns.to_string(),
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(OtlpValue {
                        string_value: "Agent exited".to_string(),
                    }),
                    attributes: Vec::new(),
                    trace_id: non_empty_trace_id(&state.trace_id),
                    span_id: None,
                });
            }
            OplogEntry::Log(params) => {
                let time_ns = datetime_to_nanos(&params.timestamp);
                counters.add(
                    "golem.log.count",
                    "1",
                    "Log message count",
                    time_ns,
                    1,
                    vec![KeyValue {
                        key: "level".to_string(),
                        value: OtlpValue {
                            string_value: log_level_severity_text(&params.level).to_string(),
                        },
                    }],
                );
                handle_log(state, params, &mut log_records);
            }
            OplogEntry::GrowMemory(params) => {
                handle_grow_memory(state, params, &mut counters, &mut metrics);
            }
            OplogEntry::HostCall(params) => {
                handle_host_call(params, &mut counters);
            }
            OplogEntry::PendingAgentInvocation(params) => {
                let time_ns = datetime_to_nanos(&params.timestamp);
                counters.add(
                    "golem.invocation.pending_count",
                    "1",
                    "Pending invocation requests",
                    time_ns,
                    1,
                    Vec::new(),
                );
            }
            OplogEntry::CreateResource(params) => {
                handle_create_resource(state, params, &mut counters, &mut metrics);
            }
            OplogEntry::DropResource(params) => {
                handle_drop_resource(state, params, &mut counters, &mut metrics);
            }
            OplogEntry::Restart(ts) => {
                let time_ns = timestamp_to_nanos(&ts);
                counters.add(
                    "golem.restart.count",
                    "1",
                    "Agent restarts",
                    time_ns,
                    1,
                    Vec::new(),
                );
            }
            OplogEntry::SuccessfulUpdate(params) => {
                handle_successful_update(params, &mut counters, &mut metrics);
            }
            OplogEntry::FailedUpdate(params) => {
                handle_failed_update(params, &mut counters);
            }
            OplogEntry::CommittedRemoteTransaction(params) => {
                handle_committed_transaction(params, &mut counters);
            }
            OplogEntry::RolledBackRemoteTransaction(params) => {
                handle_rolled_back_transaction(params, &mut counters);
            }
            OplogEntry::Snapshot(params) => {
                handle_snapshot(params, &mut metrics);
            }
            OplogEntry::OplogProcessorCheckpoint(params) => {
                handle_oplog_processor_checkpoint(params, &mut metrics);
            }
            _ => {} // ignore all other entry types
        }
    }

    counters.flush_into(&mut metrics);

    ProcessingOutput {
        spans: completed_spans,
        log_records,
        metrics,
    }
}

fn non_empty_trace_id(trace_id: &str) -> Option<String> {
    if trace_id.is_empty() {
        None
    } else {
        Some(trace_id.to_string())
    }
}

fn worker_error_variant_name(e: &golem_rust::bindings::golem::api::oplog::WorkerError) -> String {
    match e {
        golem_rust::bindings::golem::api::oplog::WorkerError::Unknown(_) => {
            "Unknown".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::InvalidRequest(_) => {
            "InvalidRequest".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::StackOverflow => {
            "StackOverflow".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::OutOfMemory => {
            "OutOfMemory".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::ExceededMemoryLimit => {
            "ExceededMemoryLimit".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::InternalError(_) => {
            "InternalError".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::DeterministicTrap(_) => {
            "DeterministicTrap".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::TransientError(_) => {
            "TransientError".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::PermanentError(_) => {
            "PermanentError".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::ExceededTableLimit => {
            "ExceededTableLimit".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::ExceededHttpCallLimit => {
            "ExceededHttpCallLimit".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::ExceededRpcCallLimit => {
            "ExceededRpcCallLimit".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::NodeOutOfFilesystemStorage => {
            "NodeOutOfFilesystemStorage".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::AgentExceededFilesystemStorageLimit => {
            "AgentExceededFilesystemStorageLimit".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::AgentTerminatedByQuota(_) => {
            "AgentTerminatedByQuota".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::EphemeralSleepTooLong(_) => {
            "EphemeralSleepTooLong".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::EphemeralFuelExhausted(_) => {
            "EphemeralFuelExhausted".to_string()
        }
        golem_rust::bindings::golem::api::oplog::WorkerError::EphemeralCannotSuspend(_) => {
            "EphemeralCannotSuspend".to_string()
        }
    }
}

fn log_level_severity_number(level: &LogLevel) -> u32 {
    match level {
        LogLevel::Stdout => 1,
        LogLevel::Stderr => 13,
        LogLevel::Trace => 1,
        LogLevel::Debug => 5,
        LogLevel::Info => 9,
        LogLevel::Warn => 13,
        LogLevel::Error => 17,
        LogLevel::Critical => 21,
    }
}

fn log_level_severity_text(level: &LogLevel) -> &'static str {
    match level {
        LogLevel::Stdout => "STDOUT",
        LogLevel::Stderr => "STDERR",
        LogLevel::Trace => "TRACE",
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
        LogLevel::Critical => "CRITICAL",
    }
}

fn handle_log(state: &WorkerState, params: LogParameters, log_records: &mut Vec<OtlpLogRecord>) {
    let time_ns = datetime_to_nanos(&params.timestamp).to_string();
    let mut attributes = Vec::new();
    if !params.context.is_empty() {
        attributes.push(KeyValue {
            key: "log.context".to_string(),
            value: OtlpValue {
                string_value: params.context,
            },
        });
    }

    log_records.push(OtlpLogRecord {
        time_unix_nano: time_ns.clone(),
        observed_time_unix_nano: time_ns,
        severity_number: log_level_severity_number(&params.level),
        severity_text: log_level_severity_text(&params.level).to_string(),
        body: Some(OtlpValue {
            string_value: params.message,
        }),
        attributes,
        trace_id: non_empty_trace_id(&state.trace_id),
        span_id: None,
    });
}

fn gauge_metric(
    name: &str,
    unit: &str,
    description: &str,
    time_ns: &str,
    value: u64,
) -> OtlpMetric {
    OtlpMetric {
        name: name.to_string(),
        unit: unit.to_string(),
        description: description.to_string(),
        sum: None,
        gauge: Some(OtlpGauge {
            data_points: vec![OtlpNumberDataPoint {
                start_time_unix_nano: time_ns.to_string(),
                time_unix_nano: time_ns.to_string(),
                as_int: Some(value.to_string()),
                as_double: None,
                attributes: Vec::new(),
            }],
        }),
    }
}

fn handle_create(
    state: &mut WorkerState,
    params: RawCreateParameters,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp).to_string();

    state.total_memory_bytes = params.initial_total_linear_memory_size;

    metrics.push(gauge_metric(
        "golem.memory.initial_bytes",
        "By",
        "Initial linear memory size",
        &time_ns,
        params.initial_total_linear_memory_size,
    ));

    metrics.push(gauge_metric(
        "golem.memory.total_bytes",
        "By",
        "Total linear memory size",
        &time_ns,
        state.total_memory_bytes,
    ));

    metrics.push(gauge_metric(
        "golem.component.size_bytes",
        "By",
        "Component size",
        &time_ns,
        params.component_size,
    ));
}

fn handle_grow_memory(
    state: &mut WorkerState,
    params: GrowMemoryParameters,
    counters: &mut CounterAccumulator,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);

    state.total_memory_bytes += params.delta;

    counters.add(
        "golem.memory.growth_bytes",
        "By",
        "Linear memory growth",
        time_ns,
        params.delta as i128,
        Vec::new(),
    );

    metrics.push(gauge_metric(
        "golem.memory.total_bytes",
        "By",
        "Total linear memory size",
        &time_ns.to_string(),
        state.total_memory_bytes,
    ));
}

fn handle_host_call(params: RawHostCallParameters, counters: &mut CounterAccumulator) {
    let time_ns = datetime_to_nanos(&params.timestamp);
    let fn_type = wrapped_function_type_name(&params.durable_function_type);
    counters.add(
        "golem.host_call.count",
        "1",
        "Host function calls",
        time_ns,
        1,
        vec![
            KeyValue {
                key: "function.name".to_string(),
                value: OtlpValue {
                    string_value: params.function_name,
                },
            },
            KeyValue {
                key: "durable_function_type".to_string(),
                value: OtlpValue {
                    string_value: fn_type.to_string(),
                },
            },
        ],
    );
}

fn handle_invocation_started(
    state: &mut WorkerState,
    params: RawAgentInvocationStartedParameters,
    counters: &mut CounterAccumulator,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);
    state.invocation_start_ns = Some(time_ns);

    counters.add(
        "golem.invocation.count",
        "1",
        "Invocation count",
        time_ns,
        1,
        Vec::new(),
    );

    if !state.pending_spans.is_empty() || !state.implicit_spans.is_empty() {
        println!(
            "OTLP exporter: new invocation started with {} pending and {} implicit spans still open, discarding",
            state.pending_spans.len(),
            state.implicit_spans.len()
        );
    }
    state.pending_spans.clear();
    state.implicit_spans.clear();
    state.terminal_error = None;
    state.inherited_span_parents.clear();

    state.trace_id = params.trace_id;
    state.trace_states = params.trace_states;

    // First pass: build a raw map of inherited local span_id → parent_span_id,
    // and collect external spans. External spans are remote parent boundaries:
    // they are not exported by this worker, but child spans can still use their
    // span id as an OTLP parent.
    let mut raw_inherited: HashMap<String, Option<String>> = HashMap::new();
    let mut external_spans: HashSet<String> = HashSet::new();

    for span_data in &params.invocation_context {
        match span_data {
            SpanData::LocalSpan(local) if local.inherited => {
                raw_inherited.insert(local.span_id.clone(), local.parent.clone());
            }
            SpanData::ExternalSpan(ext) => {
                external_spans.insert(ext.span_id.clone());
            }
            _ => {}
        }
    }

    // Resolve each inherited entry: follow the parent chain through the
    // inherited map until reaching a parent that is NOT inherited (i.e. it was
    // exported by the originating worker), an external span boundary, or None
    // (root).
    let mut resolved: HashMap<String, Option<String>> = raw_inherited
        .keys()
        .map(|span_id| {
            let resolved_parent =
                resolve_inherited_parent(span_id, &raw_inherited, &external_spans);
            (span_id.clone(), resolved_parent)
        })
        .collect();
    resolved.extend(
        external_spans
            .into_iter()
            .map(|span_id| (span_id.clone(), Some(span_id))),
    );

    state.inherited_span_parents = resolved;

    // Second pass: collect non-inherited local spans, resolving parents
    // through the inherited map when necessary.
    for span_data in params.invocation_context {
        match span_data {
            SpanData::LocalSpan(local) if !local.inherited => {
                let attrs: HashMap<String, String> = local
                    .attributes
                    .into_iter()
                    .map(|a| (a.key, attribute_value_to_string(&a.value)))
                    .collect();

                let parent =
                    resolve_parent_through_inherited(local.parent, &state.inherited_span_parents);

                state.implicit_spans.push(PendingSpan {
                    span_id: local.span_id,
                    parent_span_id: parent,
                    start_time_ns: datetime_to_nanos(&local.start),
                    attributes: attrs,
                });
            }
            _ => {}
        }
    }
}

/// Given an inherited local span id, follow the parent chain until we find a
/// parent that is NOT itself inherited (meaning it was exported by the
/// originating worker), an external span boundary, or `None` if the chain ends
/// at a root.
fn resolve_inherited_parent(
    span_id: &str,
    inherited: &HashMap<String, Option<String>>,
    external_spans: &HashSet<String>,
) -> Option<String> {
    let mut current = span_id;
    loop {
        match inherited.get(current) {
            Some(Some(parent)) => {
                if external_spans.contains(parent.as_str()) {
                    return Some(parent.clone());
                } else if inherited.contains_key(parent.as_str()) {
                    current = parent.as_str();
                } else {
                    // Parent is not inherited — it's the real ancestor
                    return Some(parent.clone());
                }
            }
            Some(None) => {
                // This entry is a root.
                return None;
            }
            None => {
                // Not in the map — shouldn't happen for the initial call
                return None;
            }
        }
    }
}

/// If `parent` points to an inherited span, resolve through the inherited map
/// to find the real (non-inherited) ancestor. Otherwise return as-is.
fn resolve_parent_through_inherited(
    parent: Option<String>,
    inherited: &HashMap<String, Option<String>>,
) -> Option<String> {
    match parent {
        Some(ref pid) if inherited.contains_key(pid.as_str()) => {
            inherited.get(pid.as_str()).cloned().flatten()
        }
        other => other,
    }
}

fn handle_start_span(state: &mut WorkerState, params: StartSpanParameters) {
    let attrs: HashMap<String, String> = params
        .attributes
        .into_iter()
        .map(|a| (a.key, attribute_value_to_string(&a.value)))
        .collect();

    let parent = resolve_parent_through_inherited(params.parent, &state.inherited_span_parents);

    state.pending_spans.insert(
        params.span_id.clone(),
        PendingSpan {
            span_id: params.span_id,
            parent_span_id: parent,
            start_time_ns: datetime_to_nanos(&params.timestamp),
            attributes: attrs,
        },
    );
}

fn handle_set_span_attribute(state: &mut WorkerState, params: SetSpanAttributeParameters) {
    let value = attribute_value_to_string(&params.value);

    if let Some(span) = state.pending_spans.get_mut(&params.span_id) {
        span.attributes.insert(params.key, value);
        return;
    }

    for span in &mut state.implicit_spans {
        if span.span_id == params.span_id {
            span.attributes.insert(params.key, value);
            return;
        }
    }

    println!(
        "OTLP exporter: set-span-attribute for unknown span {}",
        params.span_id
    );
}

fn handle_finish_span(
    state: &mut WorkerState,
    params: FinishSpanParameters,
    completed: &mut Vec<OtlpSpan>,
) {
    if let Some(span) = state.pending_spans.remove(&params.span_id) {
        let end_time_ns = datetime_to_nanos(&params.timestamp);
        let trace_state = combined_trace_state(&state.trace_states);
        completed.push(build_otel_span(
            &state.trace_id,
            trace_state.as_deref(),
            span,
            end_time_ns,
            false,
            None,
        ));
    } else {
        println!(
            "OTLP exporter: finish-span for unknown span {}",
            params.span_id
        );
    }
}

fn handle_invocation_finished(
    state: &mut WorkerState,
    params: RawAgentInvocationFinishedParameters,
    completed: &mut Vec<OtlpSpan>,
    metrics: &mut Vec<OtlpMetric>,
) {
    let end_time_ns = datetime_to_nanos(&params.timestamp);
    flush_implicit_spans(state, end_time_ns, false, completed);
    flush_remaining_explicit_spans(state, end_time_ns, false, completed);

    let time_ns = end_time_ns.to_string();

    if let Some(start_ns) = state.invocation_start_ns.take() {
        let duration_ns = end_time_ns.saturating_sub(start_ns);
        metrics.push(OtlpMetric {
            name: "golem.invocation.duration_ns".to_string(),
            unit: "ns".to_string(),
            description: "Invocation duration".to_string(),
            sum: Some(OtlpSum {
                aggregation_temporality: 1,
                is_monotonic: true,
                data_points: vec![OtlpNumberDataPoint {
                    start_time_unix_nano: time_ns.clone(),
                    time_unix_nano: time_ns.clone(),
                    as_int: Some(duration_ns.to_string()),
                    as_double: None,
                    attributes: Vec::new(),
                }],
            }),
            gauge: None,
        });
    }

    if params.consumed_fuel > 0 {
        metrics.push(OtlpMetric {
            name: "golem.invocation.fuel_consumed".to_string(),
            unit: "1".to_string(),
            description: "Fuel consumed by the invocation".to_string(),
            sum: Some(OtlpSum {
                aggregation_temporality: 1,
                is_monotonic: true,
                data_points: vec![OtlpNumberDataPoint {
                    start_time_unix_nano: time_ns.clone(),
                    time_unix_nano: time_ns,
                    as_int: Some(params.consumed_fuel.to_string()),
                    as_double: None,
                    attributes: Vec::new(),
                }],
            }),
            gauge: None,
        });
    }
}

fn handle_terminal(
    state: &mut WorkerState,
    end_time_ns: u128,
    is_error: bool,
    completed: &mut Vec<OtlpSpan>,
) {
    flush_implicit_spans(state, end_time_ns, is_error, completed);
    flush_remaining_explicit_spans(state, end_time_ns, is_error, completed);
}

fn combined_trace_state(trace_states: &[String]) -> Option<String> {
    if trace_states.is_empty() {
        None
    } else {
        Some(trace_states.join(","))
    }
}

fn flush_implicit_spans(
    state: &mut WorkerState,
    end_time_ns: u128,
    is_error: bool,
    completed: &mut Vec<OtlpSpan>,
) {
    let error_msg = state.terminal_error.clone();
    let trace_id = state.trace_id.clone();
    let trace_state = combined_trace_state(&state.trace_states);
    let spans = std::mem::take(&mut state.implicit_spans);

    for span in spans {
        completed.push(build_otel_span(
            &trace_id,
            trace_state.as_deref(),
            span,
            end_time_ns,
            is_error,
            error_msg.as_deref(),
        ));
    }
}

fn flush_remaining_explicit_spans(
    state: &mut WorkerState,
    end_time_ns: u128,
    is_error: bool,
    completed: &mut Vec<OtlpSpan>,
) {
    let error_msg = state.terminal_error.clone();
    let trace_id = state.trace_id.clone();
    let trace_state = combined_trace_state(&state.trace_states);
    let spans: Vec<PendingSpan> = state.pending_spans.drain().map(|(_, v)| v).collect();

    for span in spans {
        completed.push(build_otel_span(
            &trace_id,
            trace_state.as_deref(),
            span,
            end_time_ns,
            is_error,
            error_msg.as_deref(),
        ));
    }
}

fn handle_create_resource(
    state: &mut WorkerState,
    params: RawCreateResourceParameters,
    counters: &mut CounterAccumulator,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);

    state.active_resources += 1;

    counters.add(
        "golem.resources.created",
        "1",
        "Resource instances created",
        time_ns,
        1,
        Vec::new(),
    );

    // `golem.resources.active` reports the CURRENT count after this create, not an
    // independent event — a non-monotonic Sum. Summing two such state snapshots would
    // be meaningless, so this stays a direct per-entry push, never through the
    // monotonic-only CounterAccumulator (see counters.rs module doc).
    metrics.push(OtlpMetric {
        name: "golem.resources.active".to_string(),
        unit: "1".to_string(),
        description: "Active resource instances".to_string(),
        sum: Some(OtlpSum {
            aggregation_temporality: 1,
            is_monotonic: false,
            data_points: vec![OtlpNumberDataPoint {
                start_time_unix_nano: time_ns.to_string(),
                time_unix_nano: time_ns.to_string(),
                as_int: Some(state.active_resources.to_string()),
                as_double: None,
                attributes: Vec::new(),
            }],
        }),
        gauge: None,
    });
}

fn handle_drop_resource(
    state: &mut WorkerState,
    params: RawDropResourceParameters,
    counters: &mut CounterAccumulator,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);

    state.active_resources = (state.active_resources - 1).max(0);

    counters.add(
        "golem.resources.dropped",
        "1",
        "Resource instances dropped",
        time_ns,
        1,
        Vec::new(),
    );

    // See handle_create_resource: non-monotonic state snapshot, never aggregated.
    metrics.push(OtlpMetric {
        name: "golem.resources.active".to_string(),
        unit: "1".to_string(),
        description: "Active resource instances".to_string(),
        sum: Some(OtlpSum {
            aggregation_temporality: 1,
            is_monotonic: false,
            data_points: vec![OtlpNumberDataPoint {
                start_time_unix_nano: time_ns.to_string(),
                time_unix_nano: time_ns.to_string(),
                as_int: Some(state.active_resources.to_string()),
                as_double: None,
                attributes: Vec::new(),
            }],
        }),
        gauge: None,
    });
}

fn handle_successful_update(
    params: RawSuccessfulUpdateParameters,
    counters: &mut CounterAccumulator,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);

    counters.add(
        "golem.update.success_count",
        "1",
        "Successful component updates",
        time_ns,
        1,
        Vec::new(),
    );

    metrics.push(gauge_metric(
        "golem.component.size_bytes",
        "By",
        "Component size",
        &time_ns.to_string(),
        params.new_component_size,
    ));
}

fn handle_failed_update(params: FailedUpdateParameters, counters: &mut CounterAccumulator) {
    let time_ns = datetime_to_nanos(&params.timestamp);
    counters.add(
        "golem.update.failure_count",
        "1",
        "Failed component updates",
        time_ns,
        1,
        Vec::new(),
    );
}

fn handle_committed_transaction(
    params: RemoteTransactionParameters,
    counters: &mut CounterAccumulator,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);
    counters.add(
        "golem.transaction.committed",
        "1",
        "Committed remote transactions",
        time_ns,
        1,
        Vec::new(),
    );
}

fn handle_rolled_back_transaction(
    params: RemoteTransactionParameters,
    counters: &mut CounterAccumulator,
) {
    let time_ns = datetime_to_nanos(&params.timestamp);
    counters.add(
        "golem.transaction.rolled_back",
        "1",
        "Rolled back remote transactions",
        time_ns,
        1,
        Vec::new(),
    );
}

fn handle_snapshot(params: RawSnapshotParameters, metrics: &mut Vec<OtlpMetric>) {
    let time_ns = datetime_to_nanos(&params.timestamp).to_string();
    if let Some(size) = oplog_payload_size(&params.data) {
        metrics.push(OtlpMetric {
            name: "golem.snapshot.size_bytes".to_string(),
            unit: "By".to_string(),
            description: "Snapshot size".to_string(),
            sum: Some(OtlpSum {
                aggregation_temporality: 1,
                is_monotonic: true,
                data_points: vec![OtlpNumberDataPoint {
                    start_time_unix_nano: time_ns.clone(),
                    time_unix_nano: time_ns,
                    as_int: Some(size.to_string()),
                    as_double: None,
                    attributes: Vec::new(),
                }],
            }),
            gauge: None,
        });
    }
}

fn handle_oplog_processor_checkpoint(
    params: RawOplogProcessorCheckpointParameters,
    metrics: &mut Vec<OtlpMetric>,
) {
    let time_ns = datetime_to_nanos(&params.timestamp).to_string();
    let lag = params.sending_up_to.saturating_sub(params.confirmed_up_to);
    metrics.push(OtlpMetric {
        name: "golem.oplog_processor.lag".to_string(),
        unit: "1".to_string(),
        description: "Oplog processor delivery lag (entries)".to_string(),
        sum: None,
        gauge: Some(OtlpGauge {
            data_points: vec![OtlpNumberDataPoint {
                start_time_unix_nano: time_ns.clone(),
                time_unix_nano: time_ns,
                as_int: Some(lag.to_string()),
                as_double: None,
                attributes: Vec::new(),
            }],
        }),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use golem_rust::wasip2::clocks::wall_clock::Datetime;

    fn ts(seconds: u64) -> Datetime {
        Datetime {
            seconds,
            nanoseconds: 0,
        }
    }

    fn fresh_state() -> WorkerState {
        WorkerState {
            trace_id: String::new(),
            trace_states: Vec::new(),
            pending_spans: HashMap::new(),
            implicit_spans: Vec::new(),
            terminal_error: None,
            inherited_span_parents: HashMap::new(),
            invocation_start_ns: None,
            total_memory_bytes: 0,
            active_resources: 0,
        }
    }

    // Fix verification: a batch containing many `HostCall` entries across a few distinct
    // (function_name, durable_function_type) pairs must collapse into one merged metric
    // per distinct pair, not one metric per entry — see counters.rs module doc for why.
    // Confirms the aggregation is wired all the way through process_entries(), not just
    // unit-tested in isolation on CounterAccumulator itself.
    #[test]
    fn host_call_entries_collapse_into_one_metric_per_distinct_function() {
        let mut state = fresh_state();

        fn host_call(sec: u64, function_name: &str, fn_type: WrappedFunctionType) -> OplogEntry {
            OplogEntry::HostCall(RawHostCallParameters {
                timestamp: ts(sec),
                function_name: function_name.to_string(),
                request: OplogPayload::Inline(Vec::new()),
                response: OplogPayload::Inline(Vec::new()),
                durable_function_type: fn_type,
            })
        }

        // 50 entries across 3 distinct (function_name, durable_function_type) pairs.
        let mut entries = Vec::new();
        for i in 0..20 {
            entries.push(host_call(100 + i, "fetch", WrappedFunctionType::WriteRemote));
        }
        for i in 0..20 {
            entries.push(host_call(
                200 + i,
                "filesystem::write",
                WrappedFunctionType::WriteLocal,
            ));
        }
        for i in 0..10 {
            entries.push(host_call(
                300 + i,
                "filesystem::read",
                WrappedFunctionType::ReadLocal,
            ));
        }

        let output = process_entries(&mut state, entries);

        let host_call_metrics: Vec<_> = output
            .metrics
            .iter()
            .filter(|m| m.name == "golem.host_call.count")
            .collect();
        assert_eq!(
            host_call_metrics.len(),
            3,
            "50 host calls across 3 distinct function/type pairs must merge into exactly 3 metrics, got: {:?}",
            host_call_metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
        );

        let sums: HashMap<String, i64> = host_call_metrics
            .iter()
            .map(|m| {
                let dp = &m.sum.as_ref().unwrap().data_points[0];
                let attr = dp
                    .attributes
                    .iter()
                    .find(|a| a.key == "function.name")
                    .unwrap();
                (
                    attr.value.string_value.clone(),
                    dp.as_int.as_ref().unwrap().parse::<i64>().unwrap(),
                )
            })
            .collect();
        assert_eq!(sums.get("fetch"), Some(&20));
        assert_eq!(sums.get("filesystem::write"), Some(&20));
        assert_eq!(sums.get("filesystem::read"), Some(&10));
    }

    // Regression test for a production bug: `OtlpExporterComponent::process()` used to
    // commit `WorkerState` back to the shared cache only AFTER a successful OTLP export,
    // using `?` on the fallible send. Any export failure (e.g. the collector being briefly
    // unreachable) discarded the whole batch's derived state, silently orphaning any span
    // whose StartSpan/FinishSpan pair straddled two separate process() batches. The fix
    // commits state unconditionally, right after process_entries() runs, before any
    // network I/O is attempted. This test proves the mechanism that fix depends on: a span
    // opened in one batch is correctly completed by a later batch, as long as the SAME
    // WorkerState is carried forward between calls (exactly what the unconditional commit
    // in lib.rs now guarantees on every call, regardless of export outcome).
    #[test]
    fn span_completes_across_batches_when_state_is_carried_forward() {
        let mut state = fresh_state();

        // Batch 1: only a StartSpan — nothing completed yet, so under both the old and
        // new code this batch has nothing to export and always committed its state.
        let entries_batch_1 = vec![OplogEntry::StartSpan(StartSpanParameters {
            timestamp: ts(100),
            span_id: "s1".to_string(),
            parent: None,
            linked_context_id: None,
            attributes: Vec::new(),
        })];
        let output_1 = process_entries(&mut state, entries_batch_1);
        assert!(
            output_1.spans.is_empty(),
            "span must not be exportable before it finishes"
        );
        assert!(
            state.pending_spans.contains_key("s1"),
            "the open span must be tracked in WorkerState across batches"
        );

        // Batch 2 (a separate process() call, using the SAME carried-forward state, as
        // the fixed lib.rs now always guarantees): FinishSpan for s1.
        let entries_batch_2 = vec![OplogEntry::FinishSpan(FinishSpanParameters {
            timestamp: ts(200),
            span_id: "s1".to_string(),
        })];
        let output_2 = process_entries(&mut state, entries_batch_2);

        assert_eq!(
            output_2.spans.len(),
            1,
            "the span must complete once its FinishSpan arrives in a later batch"
        );
        assert_eq!(output_2.spans[0].span_id, "s1");
        assert!(
            state.pending_spans.is_empty(),
            "the completed span must be removed from pending state"
        );
    }

    // Documents the exact failure mode the fix prevents: if a batch's derived state is
    // NOT carried forward (e.g. discarded because an export attempt failed before the old
    // code's late WORKER_STATES commit was reached), a later FinishSpan for a span opened
    // in the lost batch can never be matched, and the span is silently dropped forever.
    #[test]
    fn finish_span_is_silently_dropped_if_prior_batch_state_was_lost() {
        let mut lost_state = fresh_state();
        let entries_batch_1 = vec![OplogEntry::StartSpan(StartSpanParameters {
            timestamp: ts(100),
            span_id: "s1".to_string(),
            parent: None,
            linked_context_id: None,
            attributes: Vec::new(),
        })];
        // Batch 1 runs and derives a pending span, but (simulating the old bug) its
        // resulting state is never committed — the next batch starts from fresh state.
        let _ = process_entries(&mut lost_state, entries_batch_1);
        let mut fresh = fresh_state();

        let entries_batch_2 = vec![OplogEntry::FinishSpan(FinishSpanParameters {
            timestamp: ts(200),
            span_id: "s1".to_string(),
        })];
        let output_2 = process_entries(&mut fresh, entries_batch_2);

        assert!(
            output_2.spans.is_empty(),
            "without carried-forward state, the span's completion is unrecoverable"
        );
    }

    #[test]
    fn resolves_inherited_chain_to_external_boundary() {
        let inherited = HashMap::from([
            ("child".to_string(), Some("parent".to_string())),
            ("parent".to_string(), Some("external".to_string())),
        ]);
        let external_spans = HashSet::from(["external".to_string()]);

        assert_eq!(
            resolve_inherited_parent("child", &inherited, &external_spans),
            Some("external".to_string())
        );
    }

    #[test]
    fn resolves_inherited_chain_to_non_inherited_parent() {
        let inherited = HashMap::from([
            ("child".to_string(), Some("parent".to_string())),
            ("parent".to_string(), Some("exported".to_string())),
        ]);
        let external_spans = HashSet::new();

        assert_eq!(
            resolve_inherited_parent("child", &inherited, &external_spans),
            Some("exported".to_string())
        );
    }

    #[test]
    fn resolves_direct_external_parent_to_external_span_id() {
        let inherited_span_parents =
            HashMap::from([("external".to_string(), Some("external".to_string()))]);

        assert_eq!(
            resolve_parent_through_inherited(
                Some("external".to_string()),
                &inherited_span_parents,
            ),
            Some("external".to_string())
        );
    }
}

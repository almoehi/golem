//! In-batch aggregation for monotonic counter metrics.
//!
//! `process_entries` converts most oplog entries into their own individual
//! `OtlpMetric` (see `processing.rs`), but many of those emissions — most
//! prominently `golem.host_call.count`, since every WASI host call produces
//! one — share the same `(name, attributes)` key within a batch and are
//! logically "N events in this window," not N independent metrics. This
//! accumulator merges same-key monotonic-counter emissions within a single
//! `process_entries` call into one summed data point, flushed once after the
//! entry loop.
//!
//! Only true monotonic delta counters belong here (see call sites in
//! `processing.rs`). Gauges and the one non-monotonic sum
//! (`golem.resources.active`) report *current state*, not deltas — summing
//! two state snapshots is meaningless, so those are pushed directly to the
//! output `Vec<OtlpMetric>` and never go through this accumulator.
//!
//! Correctness note (aggregation_temporality: DELTA): `process()` is invoked
//! per source agent, and that agent's oplog entries are strictly ordered and
//! non-overlapping across successive batches — so merging same-key points
//! within one batch into `[min(time), max(time)]` still yields a valid,
//! non-overlapping DELTA interval relative to every other batch. No switch to
//! CUMULATIVE and no cross-batch state is needed for this in-batch merge.

use crate::otlp_json::{KeyValue, OtlpMetric, OtlpNumberDataPoint, OtlpSum};
use std::collections::HashMap;

#[derive(Hash, Eq, PartialEq, Clone)]
struct CounterKey {
    name: &'static str,
    unit: &'static str,
    description: &'static str,
    // Sorted (key, value) pairs so attribute order at the call site never
    // affects grouping.
    attrs: Vec<(String, String)>,
}

struct CounterAccum {
    sum: i128,
    start_time_ns: u128,
    end_time_ns: u128,
    attributes: Vec<KeyValue>,
}

/// Accumulates monotonic counter increments within one `process_entries`
/// call, keyed by metric identity + exact attribute set. Local to a single
/// batch — see the module doc for why no cross-batch buffering is done here.
#[derive(Default)]
pub(crate) struct CounterAccumulator {
    by_key: HashMap<CounterKey, CounterAccum>,
    order: Vec<CounterKey>,
}

impl CounterAccumulator {
    /// Record `delta` (usually 1 for an event count, or a signed byte delta
    /// for e.g. memory growth) for the counter identified by `name` +
    /// `attributes`, at time `time_ns`.
    pub(crate) fn add(
        &mut self,
        name: &'static str,
        unit: &'static str,
        description: &'static str,
        time_ns: u128,
        delta: i128,
        attributes: Vec<KeyValue>,
    ) {
        let mut attrs: Vec<(String, String)> = attributes
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.string_value.clone()))
            .collect();
        attrs.sort();
        let key = CounterKey {
            name,
            unit,
            description,
            attrs,
        };

        match self.by_key.get_mut(&key) {
            Some(acc) => {
                acc.sum += delta;
                acc.start_time_ns = acc.start_time_ns.min(time_ns);
                acc.end_time_ns = acc.end_time_ns.max(time_ns);
            }
            None => {
                self.order.push(key.clone());
                self.by_key.insert(
                    key,
                    CounterAccum {
                        sum: delta,
                        start_time_ns: time_ns,
                        end_time_ns: time_ns,
                        attributes,
                    },
                );
            }
        }
    }

    /// Flush all accumulated counters into `metrics` as one merged
    /// `OtlpMetric` per key, in first-seen order (deterministic output).
    pub(crate) fn flush_into(self, metrics: &mut Vec<OtlpMetric>) {
        for key in self.order {
            // `order` only ever gets a key pushed alongside its first `by_key`
            // insert, and keys are never removed — always present.
            let acc = self
                .by_key
                .get(&key)
                .expect("CounterAccumulator: key in `order` missing from `by_key`");
            metrics.push(OtlpMetric {
                name: key.name.to_string(),
                unit: key.unit.to_string(),
                description: key.description.to_string(),
                sum: Some(OtlpSum {
                    aggregation_temporality: 1, // DELTA — see module doc
                    is_monotonic: true,
                    data_points: vec![OtlpNumberDataPoint {
                        start_time_unix_nano: acc.start_time_ns.to_string(),
                        time_unix_nano: acc.end_time_ns.to_string(),
                        as_int: Some(acc.sum.to_string()),
                        as_double: None,
                        attributes: acc.attributes.clone(),
                    }],
                }),
                gauge: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp_json::OtlpValue;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: OtlpValue {
                string_value: value.to_string(),
            },
        }
    }

    #[test]
    fn merges_same_key_increments_into_one_summed_point() {
        let mut acc = CounterAccumulator::default();
        for t in [100u128, 150, 200] {
            acc.add(
                "golem.host_call.count",
                "1",
                "Host function calls",
                t,
                1,
                vec![kv("function.name", "fetch"), kv("durable_function_type", "WriteRemote")],
            );
        }
        let mut metrics = Vec::new();
        acc.flush_into(&mut metrics);

        assert_eq!(metrics.len(), 1, "3 same-key increments must merge into 1 metric");
        let sum = metrics[0].sum.as_ref().unwrap();
        assert_eq!(sum.data_points.len(), 1);
        let dp = &sum.data_points[0];
        assert_eq!(dp.as_int.as_deref(), Some("3"));
        assert_eq!(dp.start_time_unix_nano, "100");
        assert_eq!(dp.time_unix_nano, "200");
    }

    #[test]
    fn distinct_attribute_values_never_merge() {
        let mut acc = CounterAccumulator::default();
        acc.add(
            "golem.host_call.count",
            "1",
            "Host function calls",
            100,
            1,
            vec![kv("function.name", "fetch"), kv("durable_function_type", "WriteRemote")],
        );
        acc.add(
            "golem.host_call.count",
            "1",
            "Host function calls",
            100,
            1,
            vec![kv("function.name", "filesystem::write"), kv("durable_function_type", "WriteLocal")],
        );
        let mut metrics = Vec::new();
        acc.flush_into(&mut metrics);

        assert_eq!(
            metrics.len(),
            2,
            "different function.name/durable_function_type must stay separate metrics"
        );
    }

    #[test]
    fn attribute_order_at_call_site_does_not_affect_grouping() {
        let mut acc = CounterAccumulator::default();
        acc.add(
            "golem.host_call.count",
            "1",
            "Host function calls",
            100,
            1,
            vec![kv("a", "1"), kv("b", "2")],
        );
        acc.add(
            "golem.host_call.count",
            "1",
            "Host function calls",
            100,
            1,
            vec![kv("b", "2"), kv("a", "1")],
        );
        let mut metrics = Vec::new();
        acc.flush_into(&mut metrics);

        assert_eq!(metrics.len(), 1, "same attributes in different order must still merge");
        assert_eq!(
            metrics[0].sum.as_ref().unwrap().data_points[0].as_int.as_deref(),
            Some("2")
        );
    }

    #[test]
    fn preserves_first_seen_order_across_distinct_keys() {
        let mut acc = CounterAccumulator::default();
        acc.add("golem.exit.count", "1", "Agent exits", 100, 1, Vec::new());
        acc.add("golem.error.count", "1", "Agent errors", 100, 1, Vec::new());
        acc.add("golem.exit.count", "1", "Agent exits", 150, 1, Vec::new());
        let mut metrics = Vec::new();
        acc.flush_into(&mut metrics);

        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "golem.exit.count");
        assert_eq!(metrics[1].name, "golem.error.count");
    }

    #[test]
    fn signed_delta_supports_negative_values() {
        // e.g. golem.memory.growth_bytes: not every delta is +1.
        let mut acc = CounterAccumulator::default();
        acc.add("golem.memory.growth_bytes", "By", "Linear memory growth", 100, 4096, Vec::new());
        acc.add("golem.memory.growth_bytes", "By", "Linear memory growth", 200, -1024, Vec::new());
        let mut metrics = Vec::new();
        acc.flush_into(&mut metrics);

        assert_eq!(metrics.len(), 1);
        assert_eq!(
            metrics[0].sum.as_ref().unwrap().data_points[0].as_int.as_deref(),
            Some("3072")
        );
    }
}

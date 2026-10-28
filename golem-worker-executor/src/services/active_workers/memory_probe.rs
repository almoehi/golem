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

//! Platform-abstracted probe of the executor's real memory usage and limit.
//!
//! Reports the measured resident memory and hard limit of the process's
//! environment, used as the authoritative input to admission decisions (in
//! contrast to the estimate-based semaphore in [`super::ActiveWorkers`]).
//!
//! The trait is abstract over where the limit comes from: a containerised Linux
//! deployment reads it from the cgroup, an unconstrained process reads host RAM,
//! a configured override pins it explicitly. Backend fidelity is asymmetric —
//! cgroup v2 gives the exact kernel-enforced number; other targets fall back to
//! best-effort process RSS via [`ProcessRssProbe`] until dedicated macOS and
//! Windows backends land. What is measured against a cgroup limit is
//! configurable ([`MemoryProbeSource`]): the whole cgroup, or only this process
//! when the container also runs other processes.

use crate::services::golem_config::MemoryProbeSource;
use std::fmt::Debug;

/// A snapshot of the executor environment's memory state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemorySnapshot {
    /// Hard ceiling: cgroup `memory.max` on constrained Linux, configured cap
    /// or host RAM otherwise. Reaching this with `current` triggers an
    /// OOM-kill.
    pub limit_bytes: u64,
    /// Currently-resident bytes: cgroup `memory.current` on Linux (touched
    /// pages, lagging but exact), process RSS otherwise.
    pub current_bytes: u64,
}

impl MemorySnapshot {
    /// Bytes between current usage and the hard limit. Saturating: never
    /// underflows if `current` momentarily exceeds the reported `limit`.
    pub fn headroom_bytes(&self) -> u64 {
        self.limit_bytes.saturating_sub(self.current_bytes)
    }
}

/// Reads the executor environment's real memory state. Sampled at every
/// admission attempt, including each wasmtime `memory.grow`, so it must be
/// cheap: the cgroup v2 backend is two small file reads independent of the
/// number of resident workers.
pub trait MemoryProbe: Send + Sync + Debug {
    fn snapshot(&self) -> MemorySnapshot;

    fn limit_bytes(&self) -> u64 {
        self.snapshot().limit_bytes
    }

    fn current_bytes(&self) -> u64 {
        self.snapshot().current_bytes
    }

    fn headroom_bytes(&self) -> u64 {
        self.snapshot().headroom_bytes()
    }
}

/// A probe whose limit is fixed at construction and whose current usage comes
/// from cross-platform process RSS via `sysinfo`.
///
/// This is the best-effort fallback used wherever no higher-fidelity backend
/// is available yet (notably macOS and Windows). It is also used when a
/// `system_memory_override` pins the limit explicitly.
#[derive(Debug)]
pub struct ProcessRssProbe {
    limit_bytes: u64,
}

impl ProcessRssProbe {
    pub fn new(limit_bytes: u64) -> Self {
        Self { limit_bytes }
    }

    fn current_rss() -> u64 {
        let mut sysinfo = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        sysinfo.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        sysinfo.process(pid).map(|p| p.memory()).unwrap_or_default()
    }
}

impl MemoryProbe for ProcessRssProbe {
    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot {
            limit_bytes: self.limit_bytes,
            current_bytes: Self::current_rss(),
        }
    }
}

/// A probe with a fixed limit and a fixed current usage, both set at
/// construction. Reports the same snapshot on every call regardless of the
/// host. Used by the in-process test harness, where the executor shares its
/// process (and therefore its real RSS) with the test framework and other
/// services, so a process-RSS probe cannot isolate this executor's footprint.
/// Pinning `current_bytes` to a known value (typically 0) makes the gate decide
/// purely on the granted accounting against the pinned limit, which is exact and
/// process-isolated, so memory-pressure tests are deterministic.
#[derive(Debug)]
pub struct FixedProbe {
    limit_bytes: u64,
    current_bytes: u64,
}

impl FixedProbe {
    pub fn new(limit_bytes: u64, current_bytes: u64) -> Self {
        Self {
            limit_bytes,
            current_bytes,
        }
    }
}

impl MemoryProbe for FixedProbe {
    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot {
            limit_bytes: self.limit_bytes,
            current_bytes: self.current_bytes,
        }
    }
}

/// A probe with a fixed limit whose current usage is read from a shared
/// counter the owner can move at any time. Lets the in-process test harness
/// put the gate under pressure (or relieve it) at a chosen moment of a test,
/// with the same isolation rationale as [`FixedProbe`].
#[derive(Debug)]
pub struct SharedUsageProbe {
    limit_bytes: u64,
    current_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl SharedUsageProbe {
    pub fn new(
        limit_bytes: u64,
        current_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            limit_bytes,
            current_bytes,
        }
    }
}

impl MemoryProbe for SharedUsageProbe {
    fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot {
            limit_bytes: self.limit_bytes,
            current_bytes: self
                .current_bytes
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }
}

/// The cgroup's working set: `memory.current` minus the reclaimable inactive
/// page cache (`inactive_file` in `memory.stat`), the measure kubelet uses for
/// eviction. The kernel drops inactive file pages before it OOM-kills, so they
/// are not memory the gate has to keep headroom for. Falls back to `current`
/// when `memory.stat` is unavailable or has no `inactive_file` line (e.g. under
/// gVisor), and never goes below zero.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn cgroup_working_set_bytes(current_bytes: u64, memory_stat: Option<&str>) -> u64 {
    let inactive_file = memory_stat.and_then(|stat| {
        stat.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some("inactive_file"), Some(value)) => value.parse::<u64>().ok(),
                _ => None,
            }
        })
    });
    current_bytes.saturating_sub(inactive_file.unwrap_or(0))
}

/// Linux cgroup v2 probe. Reads `memory.max` and `memory.current` from the
/// process's cgroup; in working-set mode `memory.stat` too, to leave out the
/// reclaimable page cache (see [`cgroup_working_set_bytes`]).
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct CgroupV2Probe {
    /// Resolved path to the cgroup directory, e.g. `/sys/fs/cgroup`.
    base: std::path::PathBuf,
    /// Fallback limit used when `memory.max` reads `max` (unlimited) — usually
    /// host RAM or the configured override.
    fallback_limit_bytes: u64,
    /// Report the working set instead of the raw `memory.current`.
    working_set: bool,
}

#[cfg(target_os = "linux")]
impl CgroupV2Probe {
    const DEFAULT_BASE: &'static str = "/sys/fs/cgroup";

    /// Attempts to construct a cgroup v2 probe. Returns `None` when the host is
    /// not running cgroup v2 (no unified `memory.current` at the base path), so
    /// the caller can fall back to [`ProcessRssProbe`].
    pub fn try_new(fallback_limit_bytes: u64) -> Option<Self> {
        let base = std::path::PathBuf::from(Self::DEFAULT_BASE);
        // cgroup v2 unified hierarchy exposes memory.current directly at the
        // delegated cgroup path. If it is not readable we are not on v2.
        if std::fs::read_to_string(base.join("memory.current")).is_ok() {
            Some(Self {
                base,
                fallback_limit_bytes,
                working_set: false,
            })
        } else {
            None
        }
    }

    /// Report the working set (`memory.current` minus inactive page cache)
    /// instead of the raw `memory.current`.
    pub fn measuring_working_set(mut self) -> Self {
        self.working_set = true;
        self
    }

    fn read_u64(&self, file: &str) -> Option<u64> {
        let raw = std::fs::read_to_string(self.base.join(file)).ok()?;
        raw.trim().parse::<u64>().ok()
    }

    fn read_limit(&self) -> u64 {
        // memory.max contains either a number of bytes or the literal "max".
        match std::fs::read_to_string(self.base.join("memory.max")) {
            Ok(raw) => {
                let trimmed = raw.trim();
                if trimmed == "max" {
                    self.fallback_limit_bytes
                } else {
                    trimmed.parse::<u64>().unwrap_or(self.fallback_limit_bytes)
                }
            }
            Err(_) => self.fallback_limit_bytes,
        }
    }
}

#[cfg(target_os = "linux")]
impl MemoryProbe for CgroupV2Probe {
    fn snapshot(&self) -> MemorySnapshot {
        let current = self.read_u64("memory.current").unwrap_or(0);
        let current_bytes = if self.working_set {
            let stat = std::fs::read_to_string(self.base.join("memory.stat")).ok();
            cgroup_working_set_bytes(current, stat.as_deref())
        } else {
            current
        };
        MemorySnapshot {
            limit_bytes: self.read_limit(),
            current_bytes,
        }
    }
}

/// Which probe [`default_probe`] builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeChoice {
    /// The cgroup's own `memory.max` / `memory.current`.
    Cgroup,
    /// The cgroup's `memory.max` against its working set.
    CgroupWorkingSet,
    /// This process's RSS against a fixed limit.
    ProcessRss { limit_bytes: u64 },
}

/// Decides which probe to build. `cgroup_limit_bytes` is the cgroup's limit
/// when a cgroup v2 memory controller is available (`None` otherwise).
fn choose_probe(
    memory_override: Option<u64>,
    source: MemoryProbeSource,
    cgroup_limit_bytes: Option<u64>,
    host_ram_bytes: u64,
) -> ProbeChoice {
    if let Some(limit_bytes) = memory_override {
        // The limit is self-declared and treated as an isolated budget measured
        // against this process's RSS: the executor does not assume it owns a
        // cgroup.
        return ProbeChoice::ProcessRss { limit_bytes };
    }
    match (source, cgroup_limit_bytes) {
        (MemoryProbeSource::Cgroup, Some(_)) => ProbeChoice::Cgroup,
        (MemoryProbeSource::CgroupWorkingSet, Some(_)) => ProbeChoice::CgroupWorkingSet,
        (MemoryProbeSource::Cgroup | MemoryProbeSource::CgroupWorkingSet, None) => {
            ProbeChoice::ProcessRss {
                limit_bytes: host_ram_bytes,
            }
        }
        // Measure only this process, but still against the container's limit:
        // that is what an OOM kill enforces.
        (MemoryProbeSource::Process, limit) => ProbeChoice::ProcessRss {
            limit_bytes: limit.unwrap_or(host_ram_bytes),
        },
    }
}

/// Constructs the probe the configuration asks for.
///
/// When `memory_override` is set, the limit is self-declared and measured
/// against this process's RSS. Otherwise the limit is the cgroup v2 `memory.max`
/// on Linux (host RAM elsewhere or when unlimited), and `source` decides what is
/// measured against it: the whole cgroup's `memory.current`
/// ([`MemoryProbeSource::Cgroup`], which counts every process in the container),
/// the cgroup's working set ([`MemoryProbeSource::CgroupWorkingSet`]: the same
/// without reclaimable page cache) or only this process's RSS
/// ([`MemoryProbeSource::Process`]).
pub fn default_probe(
    memory_override: Option<u64>,
    source: MemoryProbeSource,
) -> Box<dyn MemoryProbe> {
    let host_ram = {
        let mut sysinfo = sysinfo::System::new();
        sysinfo.refresh_memory();
        sysinfo.total_memory()
    };

    #[cfg(target_os = "linux")]
    let cgroup = if memory_override.is_none() {
        CgroupV2Probe::try_new(host_ram)
    } else {
        None
    };
    #[cfg(target_os = "linux")]
    let cgroup_limit = cgroup.as_ref().map(|probe| probe.snapshot().limit_bytes);
    #[cfg(not(target_os = "linux"))]
    let cgroup_limit: Option<u64> = None;

    match choose_probe(memory_override, source, cgroup_limit, host_ram) {
        choice @ (ProbeChoice::Cgroup | ProbeChoice::CgroupWorkingSet) => {
            #[cfg(target_os = "linux")]
            if let Some(probe) = cgroup {
                let probe = if choice == ProbeChoice::CgroupWorkingSet {
                    probe.measuring_working_set()
                } else {
                    probe
                };
                let snapshot = probe.snapshot();
                tracing::info!(
                    limit_bytes = snapshot.limit_bytes,
                    current_bytes = snapshot.current_bytes,
                    ?source,
                    "Memory probe: CgroupV2Probe (cgroup memory.max against {})",
                    if probe.working_set {
                        "memory.current minus inactive_file"
                    } else {
                        "memory.current"
                    }
                );
                return Box::new(probe);
            }
            unreachable!(
                "the cgroup probe ({choice:?}) is chosen only when a cgroup limit was read"
            )
        }
        ProbeChoice::ProcessRss { limit_bytes } => {
            tracing::info!(
                limit_bytes,
                ?source,
                pinned_by_override = memory_override.is_some(),
                "Memory probe: ProcessRssProbe (this process's RSS)"
            );
            Box::new(ProcessRssProbe::new(limit_bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_r::test;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn an_override_pins_the_limit_against_process_rss() {
        for source in [
            MemoryProbeSource::Cgroup,
            MemoryProbeSource::CgroupWorkingSet,
            MemoryProbeSource::Process,
        ] {
            assert_eq!(
                choose_probe(Some(2 * GIB), source, Some(12 * GIB), 64 * GIB),
                ProbeChoice::ProcessRss {
                    limit_bytes: 2 * GIB
                }
            );
        }
    }

    #[test]
    fn cgroup_source_measures_the_cgroup_when_there_is_one() {
        assert_eq!(
            choose_probe(None, MemoryProbeSource::Cgroup, Some(12 * GIB), 64 * GIB),
            ProbeChoice::Cgroup
        );
        assert_eq!(
            choose_probe(None, MemoryProbeSource::Cgroup, None, 64 * GIB),
            ProbeChoice::ProcessRss {
                limit_bytes: 64 * GIB
            }
        );
    }

    #[test]
    fn working_set_source_measures_the_cgroup_when_there_is_one() {
        assert_eq!(
            choose_probe(
                None,
                MemoryProbeSource::CgroupWorkingSet,
                Some(12 * GIB),
                64 * GIB
            ),
            ProbeChoice::CgroupWorkingSet
        );
        assert_eq!(
            choose_probe(None, MemoryProbeSource::CgroupWorkingSet, None, 64 * GIB),
            ProbeChoice::ProcessRss {
                limit_bytes: 64 * GIB
            }
        );
    }

    const MEMORY_STAT: &str = "anon 3000000000\nfile 2500000000\nkernel 120000000\n\
active_anon 2900000000\ninactive_anon 100000000\nactive_file 500000000\n\
inactive_file 2000000000\nunevictable 0\n";

    #[test]
    fn working_set_leaves_out_inactive_page_cache() {
        assert_eq!(
            cgroup_working_set_bytes(5_700_000_000, Some(MEMORY_STAT)),
            3_700_000_000
        );
    }

    #[test]
    fn working_set_is_current_without_a_usable_memory_stat() {
        // No memory.stat (e.g. gVisor), one without inactive_file, or a garbled
        // value: fall back to the plain memory.current reading.
        assert_eq!(cgroup_working_set_bytes(5_000, None), 5_000);
        assert_eq!(
            cgroup_working_set_bytes(5_000, Some("anon 4000\nfile 1000\n")),
            5_000
        );
        assert_eq!(
            cgroup_working_set_bytes(5_000, Some("inactive_file lots\n")),
            5_000
        );
        assert_eq!(cgroup_working_set_bytes(5_000, Some("")), 5_000);
    }

    #[test]
    fn working_set_matches_the_whole_inactive_file_key_only() {
        // `total_inactive_file` (cgroup v1 style) is a different key.
        assert_eq!(
            cgroup_working_set_bytes(
                5_000,
                Some("total_inactive_file 4000\ninactive_file 1000\n")
            ),
            4_000
        );
    }

    #[test]
    fn working_set_never_goes_below_zero() {
        assert_eq!(
            cgroup_working_set_bytes(1_000, Some("inactive_file 5000\n")),
            0
        );
    }

    #[test]
    fn process_source_measures_this_process_against_the_cgroup_limit() {
        assert_eq!(
            choose_probe(None, MemoryProbeSource::Process, Some(12 * GIB), 64 * GIB),
            ProbeChoice::ProcessRss {
                limit_bytes: 12 * GIB
            }
        );
        assert_eq!(
            choose_probe(None, MemoryProbeSource::Process, None, 64 * GIB),
            ProbeChoice::ProcessRss {
                limit_bytes: 64 * GIB
            }
        );
    }

    #[test]
    fn the_process_probe_reads_this_processs_resident_memory() {
        // Touch a buffer so the process is certainly resident beyond it.
        let buffer = vec![1u8; 32 * 1024 * 1024];
        let snapshot = default_probe(None, MemoryProbeSource::Process).snapshot();
        assert!(snapshot.current_bytes >= buffer.len() as u64);
        assert!(snapshot.limit_bytes > snapshot.current_bytes);
        drop(buffer);
    }
}

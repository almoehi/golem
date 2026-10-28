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

/// The linear memory a resident worker's memory grant already covers.
///
/// A worker reserves its last known linear memory size with the admission gate
/// when it becomes resident, and each grow beyond that afterwards. The grant is
/// held until the worker unloads — across in-place restarts of its instance (a
/// retry, an interrupt, the restart of a manual update that loads the snapshot
/// into the new revision). A restarted instance starts small again and grows
/// back into memory the grant already covers; only growth beyond the covered
/// size needs new memory from the gate. Without this, an in-place restart
/// charged all of its regrowth a second time — a manual update's snapshot load,
/// whose known size restarts at the component's initial memory, was charged on
/// top of the grant the agent already held, so a full gate refused it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinearMemoryCoverage {
    covered_bytes: u64,
}

impl LinearMemoryCoverage {
    /// Coverage of a grant made for `covered_bytes` of linear memory.
    pub fn new(covered_bytes: u64) -> Self {
        Self { covered_bytes }
    }

    /// How many bytes a grow to `new_total` linear memory has to reserve with
    /// the gate: the part beyond what the grant covers.
    pub fn uncovered_growth(&self, new_total: u64) -> u64 {
        new_total.saturating_sub(self.covered_bytes)
    }

    /// Record that the grant now covers `new_total` bytes (after reserving the
    /// uncovered growth). Never shrinks: the grant is not reduced either.
    pub fn cover(&mut self, new_total: u64) {
        self.covered_bytes = self.covered_bytes.max(new_total);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_r::test;

    #[test]
    fn growth_within_the_grant_needs_nothing_new() {
        let coverage = LinearMemoryCoverage::new(100);
        assert_eq!(coverage.uncovered_growth(40), 0);
        assert_eq!(coverage.uncovered_growth(100), 0);
    }

    #[test]
    fn only_growth_beyond_the_grant_is_charged() {
        let coverage = LinearMemoryCoverage::new(100);
        assert_eq!(coverage.uncovered_growth(130), 30);
    }

    #[test]
    fn a_restarted_instance_regrowing_is_covered_by_the_grant() {
        // The instance grew to 100 and its grant covers it. After an in-place
        // restart it grows again from its initial size: 10 -> 60 -> 100 needs
        // nothing new, and only 100 -> 120 is charged.
        let mut coverage = LinearMemoryCoverage::new(10);
        assert_eq!(coverage.uncovered_growth(100), 90);
        coverage.cover(100);

        assert_eq!(coverage.uncovered_growth(60), 0);
        coverage.cover(60);
        assert_eq!(coverage.uncovered_growth(100), 0);
        assert_eq!(coverage.uncovered_growth(120), 20);
        coverage.cover(120);
        assert_eq!(coverage, LinearMemoryCoverage::new(120));
    }
}

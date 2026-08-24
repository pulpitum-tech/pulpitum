/// Shared workload profile for the showcase and Jepsen-style fault harness.
///
/// The four one-second intervals total four times the configured target, so the
/// target is the average rate while individual intervals deliberately spike.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpikySqlLoadProfile {
    target_rps: usize,
}

impl SpikySqlLoadProfile {
    pub const DEFAULT_TARGET_RPS: usize = 50;
    pub const WORKERS: usize = 128;
    pub const QUEUE_CAPACITY: usize = 4_096;
    pub const SHAPE_PERMILLE: [usize; 4] = [250, 500, 2_250, 1_000];

    /// Creates a profile with the requested average operations per second.
    /// A zero target is normalized to the default so callers cannot silently
    /// disable the workload through an invalid environment value.
    pub fn new(target_rps: usize) -> Self {
        Self {
            target_rps: if target_rps == 0 {
                Self::DEFAULT_TARGET_RPS
            } else {
                target_rps
            },
        }
    }

    pub fn target_rps(self) -> usize {
        self.target_rps
    }

    /// Number of operations offered at the beginning of an interval.
    pub fn operations_for_second(self, second: usize) -> usize {
        let second = second % Self::SHAPE_PERMILLE.len();
        let base = self.target_rps.saturating_mul(Self::SHAPE_PERMILLE[second]) / 1_000;
        // Integer rounding leaves one or more requests unallocated at small
        // targets. Assign the remainder to the peak interval so the complete
        // four-second cycle still averages exactly to `target_rps`.
        let allocated: usize = Self::SHAPE_PERMILLE
            .iter()
            .map(|weight| self.target_rps.saturating_mul(*weight) / 1_000)
            .sum();
        let remainder = self
            .target_rps
            .saturating_mul(Self::SHAPE_PERMILLE.len())
            .saturating_sub(allocated);
        if second == 2 { base + remainder } else { base }
    }

    /// Alternates hot-bucket queries and appends to exercise both SQL paths.
    pub fn is_query(self, sequence: u64) -> bool {
        sequence.is_multiple_of(2)
    }
}

impl Default for SpikySqlLoadProfile {
    fn default() -> Self {
        Self::new(Self::DEFAULT_TARGET_RPS)
    }
}

#[cfg(test)]
mod tests {
    use super::SpikySqlLoadProfile;

    #[test]
    fn default_profile_averages_to_its_target() {
        let profile = SpikySqlLoadProfile::default();
        let total: usize = (0..SpikySqlLoadProfile::SHAPE_PERMILLE.len())
            .map(|second| profile.operations_for_second(second))
            .sum();
        assert_eq!(
            total / SpikySqlLoadProfile::SHAPE_PERMILLE.len(),
            profile.target_rps()
        );
        assert_eq!(profile.operations_for_second(2), 113);
    }
}

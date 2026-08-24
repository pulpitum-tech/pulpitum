use crate::BucketId;
use std::sync::Arc;

/// Archive failure stages are intentionally finite metric labels. Do not attach
/// shard IDs, object keys, record IDs, or error strings to metric attributes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveStage {
    Snapshot,
    Upload,
    DeleteHot,
}

/// Durable archival-job phases proposed for the recovery coordinator. These
/// are intentionally finite metric labels; job, table, bucket, lease, and
/// object identifiers must not be emitted as attributes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorPhase {
    Queued,
    Claimed,
    Uploading,
    UploadedVerified,
    PublishedCleanupPending,
    Completed,
    RetryScheduled,
    FailedNeedsAttention,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadTier {
    Hot,
    Archive,
}

/// Instrumentation boundary for the sidecar. Implementations must use bounded
/// cardinality labels: `bucket_strategy`, `tier`, `stage`, and coordinator
/// `phase` are safe; shard IDs and bucket keys are not. Application tracing belongs in the embedding
/// Node/Rust service.
///
/// Coordinator hooks describe one bounded discovery/recovery cycle. A future
/// recovery runner can call `coordinator_phase_entered` for each job transition
/// and periodically report its full phase snapshot with
/// `coordinator_phase_count`; neither accepts a dynamic job identifier.
pub trait Telemetry: Send + Sync {
    fn archive_started(&self, _bucket: &BucketId) {}
    fn archive_completed(&self, _bucket: &BucketId, _records: usize, _elapsed_seconds: f64) {}
    fn archive_failed(&self, _bucket: &BucketId, _stage: ArchiveStage) {}
    fn read_routed(&self, _bucket: &BucketId, _tier: ReadTier) {}
    fn bucket_tier_counts(&self, _hot: u64, _archiving: u64, _archive: u64) {}

    /// Records the start of one bounded coordinator discovery/recovery cycle.
    fn coordinator_cycle_started(&self) {}
    /// Records a successfully completed coordinator cycle and its elapsed time.
    fn coordinator_cycle_completed(&self, _elapsed_seconds: f64) {}
    /// Records a failed coordinator cycle and its elapsed time.
    fn coordinator_cycle_failed(&self, _elapsed_seconds: f64) {}
    /// Records entry into one finite durable archival-job phase.
    fn coordinator_phase_entered(&self, _phase: CoordinatorPhase) {}
    /// Records the current number of jobs in one finite phase.
    fn coordinator_phase_count(&self, _phase: CoordinatorPhase, _count: u64) {}
}

#[derive(Default)]
pub struct NoopTelemetry;
impl Telemetry for NoopTelemetry {}

pub type SharedTelemetry = Arc<dyn Telemetry>;

#[cfg(feature = "opentelemetry")]
pub mod otel {
    use super::{ArchiveStage, CoordinatorPhase, ReadTier, Telemetry};
    use crate::BucketId;
    use opentelemetry::{
        KeyValue, global,
        metrics::{Counter, Gauge, Histogram},
    };

    /// Emits metrics to the application's globally configured OpenTelemetry
    /// meter provider. Exporter setup is intentionally outside this crate so a
    /// Node/Rust deployment controls OTLP endpoint, credentials, and sampling.
    pub struct OtelTelemetry {
        archive_runs: Counter<u64>,
        archive_failures: Counter<u64>,
        archive_records: Counter<u64>,
        archive_duration: Histogram<f64>,
        routed_reads: Counter<u64>,
        buckets: Gauge<u64>,
        coordinator_cycles: Counter<u64>,
        coordinator_duration: Histogram<f64>,
        coordinator_phase_transitions: Counter<u64>,
        coordinator_jobs: Gauge<u64>,
    }

    impl OtelTelemetry {
        pub fn new() -> Self {
            let meter = global::meter("pulpitum");
            Self {
                archive_runs: meter.u64_counter("pulpitum.archive.runs").build(),
                archive_failures: meter.u64_counter("pulpitum.archive.failures").build(),
                archive_records: meter.u64_counter("pulpitum.archive.records").build(),
                archive_duration: meter
                    .f64_histogram("pulpitum.archive.duration")
                    .with_unit("s")
                    .build(),
                routed_reads: meter.u64_counter("pulpitum.query.routes").build(),
                buckets: meter.u64_gauge("pulpitum.buckets").build(),
                coordinator_cycles: meter
                    .u64_counter("pulpitum.archive.coordinator.cycles")
                    .build(),
                coordinator_duration: meter
                    .f64_histogram("pulpitum.archive.coordinator.duration")
                    .with_unit("s")
                    .build(),
                coordinator_phase_transitions: meter
                    .u64_counter("pulpitum.archive.coordinator.phase.transitions")
                    .build(),
                coordinator_jobs: meter.u64_gauge("pulpitum.archive.coordinator.jobs").build(),
            }
        }
    }

    impl Default for OtelTelemetry {
        fn default() -> Self {
            Self::new()
        }
    }

    fn bucket_strategy(bucket: &BucketId) -> KeyValue {
        KeyValue::new("pulpitum.bucket.strategy", bucket.strategy.as_str())
    }
    fn stage(stage: ArchiveStage) -> KeyValue {
        KeyValue::new(
            "pulpitum.archive.stage",
            match stage {
                ArchiveStage::Snapshot => "snapshot",
                ArchiveStage::Upload => "upload",
                ArchiveStage::DeleteHot => "delete_hot",
            },
        )
    }
    fn coordinator_phase(phase: CoordinatorPhase) -> KeyValue {
        KeyValue::new(
            "pulpitum.archive.coordinator.phase",
            match phase {
                CoordinatorPhase::Queued => "queued",
                CoordinatorPhase::Claimed => "claimed",
                CoordinatorPhase::Uploading => "uploading",
                CoordinatorPhase::UploadedVerified => "uploaded_verified",
                CoordinatorPhase::PublishedCleanupPending => "published_cleanup_pending",
                CoordinatorPhase::Completed => "completed",
                CoordinatorPhase::RetryScheduled => "retry_scheduled",
                CoordinatorPhase::FailedNeedsAttention => "failed_needs_attention",
            },
        )
    }
    fn coordinator_outcome(outcome: &'static str) -> KeyValue {
        KeyValue::new("pulpitum.archive.coordinator.outcome", outcome)
    }

    impl Telemetry for OtelTelemetry {
        fn archive_started(&self, bucket: &BucketId) {
            self.archive_runs.add(
                1,
                &[
                    bucket_strategy(bucket),
                    KeyValue::new("pulpitum.archive.outcome", "started"),
                ],
            );
        }
        fn archive_completed(&self, bucket: &BucketId, records: usize, elapsed_seconds: f64) {
            let attrs = [
                bucket_strategy(bucket),
                KeyValue::new("pulpitum.archive.outcome", "success"),
            ];
            self.archive_runs.add(1, &attrs);
            self.archive_records.add(records as u64, &attrs);
            self.archive_duration.record(elapsed_seconds, &attrs);
        }
        fn archive_failed(&self, bucket: &BucketId, failed_stage: ArchiveStage) {
            self.archive_failures
                .add(1, &[bucket_strategy(bucket), stage(failed_stage)]);
        }
        fn read_routed(&self, bucket: &BucketId, tier: ReadTier) {
            self.routed_reads.add(
                1,
                &[
                    bucket_strategy(bucket),
                    KeyValue::new(
                        "pulpitum.query.tier",
                        match tier {
                            ReadTier::Hot => "hot",
                            ReadTier::Archive => "archive",
                        },
                    ),
                ],
            );
        }
        fn bucket_tier_counts(&self, hot: u64, archiving: u64, archive: u64) {
            for (tier, count) in [("hot", hot), ("archiving", archiving), ("archive", archive)] {
                self.buckets
                    .record(count, &[KeyValue::new("pulpitum.bucket.tier", tier)]);
            }
        }
        fn coordinator_cycle_started(&self) {
            self.coordinator_cycles
                .add(1, &[coordinator_outcome("started")]);
        }
        fn coordinator_cycle_completed(&self, elapsed_seconds: f64) {
            let attrs = [coordinator_outcome("success")];
            self.coordinator_cycles.add(1, &attrs);
            self.coordinator_duration.record(elapsed_seconds, &attrs);
        }
        fn coordinator_cycle_failed(&self, elapsed_seconds: f64) {
            let attrs = [coordinator_outcome("failure")];
            self.coordinator_cycles.add(1, &attrs);
            self.coordinator_duration.record(elapsed_seconds, &attrs);
        }
        fn coordinator_phase_entered(&self, phase: CoordinatorPhase) {
            self.coordinator_phase_transitions
                .add(1, &[coordinator_phase(phase)]);
        }
        fn coordinator_phase_count(&self, phase: CoordinatorPhase, count: u64) {
            self.coordinator_jobs
                .record(count, &[coordinator_phase(phase)]);
        }
    }
}

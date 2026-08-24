use crate::{
    ArchiveScan, ArchiveWork, CoordinatorPhase, DurableArchiveError, DurableBucketStoreError,
    SharedArchiveStore, SharedDurableBucketStore, SharedTelemetry,
};
use chrono::{DateTime, Utc};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::Instrument;

/// Configuration for one bounded archival coordinator cycle.
#[derive(Clone, Debug)]
pub struct ArchiveRecoveryConfig {
    /// Buckets whose exclusive end is at or before this cutoff are eligible.
    pub eligible_before: DateTime<Utc>,
    /// Maximum discovered work items per cycle.
    pub scan_limit: u32,
    /// Ownership lease for snapshot, upload, publication, and cleanup.
    pub lease_for: Duration,
    /// Interval between fenced ownership-lease renewals while work is active.
    pub lease_renewal_interval: Duration,
    /// Delay before retrying a transient upload/snapshot/cleanup failure.
    pub retry_backoff: Duration,
}

impl ArchiveRecoveryConfig {
    pub fn validate(&self) -> Result<(), ArchiveRecoveryError> {
        if self.scan_limit == 0
            || self.lease_for.is_zero()
            || self.lease_renewal_interval.is_zero()
            || self.lease_renewal_interval >= self.lease_for
            || self.retry_backoff.is_zero()
        {
            return Err(ArchiveRecoveryError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ArchiveRecoveryError {
    #[error(
        "archive recovery configuration requires a non-zero scan limit, lease, renewal interval shorter than the lease, and retry backoff"
    )]
    InvalidConfig,
    #[error(transparent)]
    DurableStore(#[from] DurableBucketStoreError),
    #[error(transparent)]
    Archive(#[from] DurableArchiveError),
}

/// Summary for one bounded discovery/recovery cycle.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArchiveRecoveryOutcome {
    pub discovered: u32,
    pub claimed: u32,
    pub completed: u32,
    pub deferred: u32,
}

/// Deployment-owned archival recovery coordinator.
///
/// This runner intentionally owns no process-local job state. Every invocation
/// discovers durable work, claims it with an expiring fence, and either commits
/// it or persists a retry. Multiple replicas may run concurrently; a losing
/// replica observes `None` from a claim and leaves the work to its winner.
pub struct DurableArchiveRecoveryRunner {
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    config: ArchiveRecoveryConfig,
    telemetry: SharedTelemetry,
}

pub(crate) struct ArchiveLeaseRenewer {
    stop: Option<oneshot::Sender<()>>,
    failure: oneshot::Receiver<DurableBucketStoreError>,
    task: Option<JoinHandle<()>>,
}

impl ArchiveLeaseRenewer {
    pub(crate) fn start(
        store: SharedDurableBucketStore,
        session: Arc<crate::ArchiveSession>,
        lease_for: Duration,
        interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = oneshot::channel();
        let (failed, failure) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut failed = Some(failed);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = tokio::time::sleep(interval) => {
                        if let Err(error) = store.renew_archive_lease(&session, lease_for).await {
                            if let Some(failed) = failed.take() {
                                let _ = failed.send(error);
                            }
                            break;
                        }
                    }
                }
            }
        });
        Self {
            stop: Some(stop),
            failure,
            task: Some(task),
        }
    }

    pub(crate) async fn supervise<F, T, E>(
        &mut self,
        operation: F,
    ) -> Result<Result<T, E>, DurableBucketStoreError>
    where
        F: Future<Output = Result<T, E>>,
    {
        let mut operation = Box::pin(operation);
        tokio::select! {
            biased;
            renewal = &mut self.failure => {
                let error = renewal.unwrap_or(DurableBucketStoreError::OperationFailed);
                let _ = operation.await;
                Err(error)
            }
            result = &mut operation => Ok(result),
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ArchiveLeaseRenewer {
    fn drop(&mut self) {
        self.stop.take();
    }
}

impl DurableArchiveRecoveryRunner {
    pub fn new(
        store: SharedDurableBucketStore,
        archive: SharedArchiveStore,
        config: ArchiveRecoveryConfig,
    ) -> Result<Self, ArchiveRecoveryError> {
        Self::with_telemetry(store, archive, config, Arc::new(crate::NoopTelemetry))
    }

    pub fn with_telemetry(
        store: SharedDurableBucketStore,
        archive: SharedArchiveStore,
        config: ArchiveRecoveryConfig,
        telemetry: SharedTelemetry,
    ) -> Result<Self, ArchiveRecoveryError> {
        config.validate()?;
        Ok(Self {
            store,
            archive,
            config,
            telemetry,
        })
    }

    /// Discovers and processes at most `scan_limit` durable work items.
    #[tracing::instrument(name = "pulpitum.archive.coordinator.cycle", skip(self), err, fields(pulpitum.archive.operation = "recovery_cycle"))]
    pub async fn run_once(&self) -> Result<ArchiveRecoveryOutcome, ArchiveRecoveryError> {
        let started = Instant::now();
        self.telemetry.coordinator_cycle_started();
        let work = match self
            .store
            .discover_archive_work(ArchiveScan {
                eligible_before: self.config.eligible_before,
                limit: self.config.scan_limit,
            })
            .await
        {
            Ok(work) => work,
            Err(error) => {
                self.telemetry
                    .coordinator_cycle_failed(started.elapsed().as_secs_f64());
                return Err(error.into());
            }
        };

        let mut outcome = ArchiveRecoveryOutcome {
            discovered: work.len() as u32,
            ..ArchiveRecoveryOutcome::default()
        };
        self.telemetry
            .coordinator_phase_count(CoordinatorPhase::Queued, u64::from(outcome.discovered));

        for item in work {
            let result = match item {
                ArchiveWork::Cleanup(bucket) => self.recover_cleanup(bucket, &mut outcome).await,
                ArchiveWork::Cutover(bucket) => self.recover_cutover(bucket, &mut outcome).await,
            };
            if let Err(error) = result {
                self.telemetry
                    .coordinator_cycle_failed(started.elapsed().as_secs_f64());
                return Err(error);
            }
        }

        self.telemetry
            .coordinator_cycle_completed(started.elapsed().as_secs_f64());
        Ok(outcome)
    }

    async fn recover_cutover(
        &self,
        bucket: crate::BucketId,
        outcome: &mut ArchiveRecoveryOutcome,
    ) -> Result<(), ArchiveRecoveryError> {
        let Some(session) = self
            .store
            .claim_archive(&bucket, self.config.lease_for)
            .instrument(tracing::info_span!(
                "pulpitum.archive.coordinator.claim",
                pulpitum.archive.operation = "claim",
                pulpitum.archive.coordinator.phase = "claimed",
            ))
            .await?
        else {
            return Ok(());
        };
        outcome.claimed += 1;
        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::Claimed);

        let session = Arc::new(session);
        let mut renewer = ArchiveLeaseRenewer::start(
            Arc::clone(&self.store),
            Arc::clone(&session),
            self.config.lease_for,
            self.config.lease_renewal_interval,
        );
        let result = self
            .recover_claimed_cutover(&bucket, &session, &mut renewer, outcome)
            .await;
        renewer.shutdown().await;
        result
    }

    async fn recover_claimed_cutover(
        &self,
        bucket: &crate::BucketId,
        session: &crate::ArchiveSession,
        renewer: &mut ArchiveLeaseRenewer,
        outcome: &mut ArchiveRecoveryOutcome,
    ) -> Result<(), ArchiveRecoveryError> {
        let snapshot = self.store.snapshot(session).instrument(tracing::info_span!(
            "pulpitum.archive.coordinator.snapshot",
            pulpitum.archive.operation = "snapshot",
            pulpitum.archive.coordinator.phase = "claimed",
        ));
        let records = match renewer.supervise(snapshot).await? {
            Ok(records) => records,
            Err(error) => {
                return self.defer_cutover(session, renewer, outcome, error).await;
            }
        };

        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::Uploading);
        let upload = self
            .archive
            .put_bucket_generation(bucket, session.generation(), &records)
            .instrument(tracing::info_span!(
                "pulpitum.archive.coordinator.upload",
                pulpitum.archive.operation = "upload",
                pulpitum.archive.coordinator.phase = "uploading",
            ));
        let object_key = match renewer.supervise(upload).await? {
            Ok(key) => key,
            Err(error) => {
                self.defer_cutover(
                    session,
                    renewer,
                    outcome,
                    DurableBucketStoreError::OperationFailed,
                )
                .await?;
                return Err(DurableArchiveError::Store(error).into());
            }
        };
        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::UploadedVerified);

        let publish =
            self.store
                .publish_archive(session, object_key)
                .instrument(tracing::info_span!(
                    "pulpitum.archive.coordinator.publish",
                    pulpitum.archive.operation = "publish",
                    pulpitum.archive.coordinator.phase = "uploaded_verified",
                ));
        if let Err(error) = renewer.supervise(publish).await? {
            // Publication may have committed despite the client observing an
            // error. Durable routing state decides whether to recover cleanup
            // or reopen the pre-publication attempt.
            match renewer.supervise(self.store.state(bucket)).await?? {
                crate::BucketState::Archived {
                    hot_deleted: false, ..
                } => {
                    return self.finish_cleanup(session, renewer, outcome).await;
                }
                crate::BucketState::Archiving => {
                    return self.defer_cutover(session, renewer, outcome, error).await;
                }
                _ => return Err(error.into()),
            }
        }
        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::PublishedCleanupPending);
        self.finish_cleanup(session, renewer, outcome).await
    }

    async fn recover_cleanup(
        &self,
        bucket: crate::BucketId,
        outcome: &mut ArchiveRecoveryOutcome,
    ) -> Result<(), ArchiveRecoveryError> {
        let Some(session) = self
            .store
            .claim_cleanup(&bucket, self.config.lease_for)
            .instrument(tracing::info_span!(
                "pulpitum.archive.coordinator.claim_cleanup",
                pulpitum.archive.operation = "claim_cleanup",
                pulpitum.archive.coordinator.phase = "published_cleanup_pending",
            ))
            .await?
        else {
            return Ok(());
        };
        outcome.claimed += 1;
        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::Claimed);

        let session = Arc::new(session);
        let mut renewer = ArchiveLeaseRenewer::start(
            Arc::clone(&self.store),
            Arc::clone(&session),
            self.config.lease_for,
            self.config.lease_renewal_interval,
        );
        let result = self.finish_cleanup(&session, &mut renewer, outcome).await;
        renewer.shutdown().await;
        result
    }

    async fn finish_cleanup(
        &self,
        session: &crate::ArchiveSession,
        renewer: &mut ArchiveLeaseRenewer,
        outcome: &mut ArchiveRecoveryOutcome,
    ) -> Result<(), ArchiveRecoveryError> {
        let cleanup = self
            .store
            .delete_hot_bucket(session)
            .instrument(tracing::info_span!(
                "pulpitum.archive.coordinator.cleanup",
                pulpitum.archive.operation = "cleanup",
                pulpitum.archive.coordinator.phase = "published_cleanup_pending",
            ));
        match renewer.supervise(cleanup).await? {
            Ok(()) => {
                outcome.completed += 1;
                self.telemetry
                    .coordinator_phase_entered(CoordinatorPhase::Completed);
                Ok(())
            }
            Err(error) => {
                let defer = self
                    .store
                    .defer_cleanup(session, self.config.retry_backoff)
                    .instrument(tracing::info_span!(
                        "pulpitum.archive.coordinator.retry_cleanup",
                        pulpitum.archive.operation = "retry_cleanup",
                        pulpitum.archive.coordinator.phase = "retry_scheduled",
                    ));
                renewer.supervise(defer).await??;
                outcome.deferred += 1;
                self.telemetry
                    .coordinator_phase_entered(CoordinatorPhase::RetryScheduled);
                Err(error.into())
            }
        }
    }

    async fn defer_cutover(
        &self,
        session: &crate::ArchiveSession,
        renewer: &mut ArchiveLeaseRenewer,
        outcome: &mut ArchiveRecoveryOutcome,
        error: DurableBucketStoreError,
    ) -> Result<(), ArchiveRecoveryError> {
        let defer = self
            .store
            .defer_archive(session, self.config.retry_backoff)
            .instrument(tracing::info_span!(
                "pulpitum.archive.coordinator.retry_cutover",
                pulpitum.archive.operation = "retry_cutover",
                pulpitum.archive.coordinator.phase = "retry_scheduled",
            ));
        renewer.supervise(defer).await??;
        outcome.deferred += 1;
        self.telemetry
            .coordinator_phase_entered(CoordinatorPhase::RetryScheduled);
        Err(error.into())
    }
}

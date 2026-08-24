use crate::{
    BucketId, DurableBucketStoreError, NoopTelemetry, SharedArchiveStore, SharedDurableBucketStore,
    SharedTelemetry, StoreError, durable_archive_recovery::ArchiveLeaseRenewer,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const DIRECT_ARCHIVE_LEASE: Duration = Duration::from_secs(5 * 60);
const DIRECT_ARCHIVE_LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(60);
use thiserror::Error;

/// Result of a completed archive cutover.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveOutcome {
    pub bucket: BucketId,
    pub object_key: String,
    pub records_archived: usize,
}

#[derive(Debug, Error)]
pub enum DurableArchiveError {
    #[error(transparent)]
    DurableStore(#[from] DurableBucketStoreError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Fenced archival coordinator for [`crate::DurableTable`].
///
/// The coupled store owns the write fence, stable snapshot, conditional archive
/// publication, and atomic hot-data deletion. This coordinator only performs
/// the object-store upload between the fenced snapshot and conditional publish.
pub struct DurableArchiveCoordinator {
    store: SharedDurableBucketStore,
    archive: SharedArchiveStore,
    telemetry: SharedTelemetry,
}

impl DurableArchiveCoordinator {
    pub fn new(store: SharedDurableBucketStore, archive: SharedArchiveStore) -> Self {
        Self::with_telemetry(store, archive, Arc::new(NoopTelemetry))
    }

    pub fn with_telemetry(
        store: SharedDurableBucketStore,
        archive: SharedArchiveStore,
        telemetry: SharedTelemetry,
    ) -> Self {
        Self {
            store,
            archive,
            telemetry,
        }
    }

    /// Executes one fenced archive cutover.
    ///
    /// Snapshot and upload failures reopen the bucket through the active
    /// session. Failures after upload deliberately do not abort: publication or
    /// deletion may already have committed, and only the session owner can make
    /// a safe recovery decision.
    #[tracing::instrument(name = "pulpitum.durable_archive.cutover", skip(self, bucket), err, fields(pulpitum.archive.operation = "cutover"))]
    pub async fn archive_bucket(
        &self,
        bucket: BucketId,
    ) -> Result<ArchiveOutcome, DurableArchiveError> {
        let started = Instant::now();
        self.telemetry.archive_started(&bucket);
        let session = Arc::new(self.store.begin_archive(&bucket).await?);
        let mut renewer = ArchiveLeaseRenewer::start(
            Arc::clone(&self.store),
            Arc::clone(&session),
            DIRECT_ARCHIVE_LEASE,
            DIRECT_ARCHIVE_LEASE_RENEWAL_INTERVAL,
        );
        let result = self
            .archive_claimed_bucket(&bucket, &session, &mut renewer, started)
            .await;
        renewer.shutdown().await;
        result
    }

    async fn archive_claimed_bucket(
        &self,
        bucket: &BucketId,
        session: &crate::ArchiveSession,
        renewer: &mut ArchiveLeaseRenewer,
        started: Instant,
    ) -> Result<ArchiveOutcome, DurableArchiveError> {
        let records = match renewer.supervise(self.store.snapshot(session)).await? {
            Ok(records) => records,
            Err(error) => {
                self.telemetry
                    .archive_failed(bucket, crate::ArchiveStage::Snapshot);
                renewer
                    .supervise(self.store.abort_archive(session))
                    .await??;
                return Err(error.into());
            }
        };
        let object_key = match renewer
            .supervise(
                self.archive
                    .put_bucket_generation(bucket, session.generation(), &records),
            )
            .await?
        {
            Ok(key) => key,
            Err(error) => {
                self.telemetry
                    .archive_failed(bucket, crate::ArchiveStage::Upload);
                renewer
                    .supervise(self.store.abort_archive(session))
                    .await??;
                return Err(error.into());
            }
        };

        renewer
            .supervise(self.store.publish_archive(session, object_key.clone()))
            .await??;
        if let Err(error) = renewer
            .supervise(self.store.delete_hot_bucket(session))
            .await?
        {
            self.telemetry
                .archive_failed(bucket, crate::ArchiveStage::DeleteHot);
            return Err(error.into());
        }
        self.telemetry
            .archive_completed(bucket, records.len(), started.elapsed().as_secs_f64());

        Ok(ArchiveOutcome {
            bucket: bucket.clone(),
            object_key,
            records_archived: records.len(),
        })
    }
}

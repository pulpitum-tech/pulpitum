//! Deliberately failing adversarial tests.
//!
//! Run with `cargo test --test known_failures -- --ignored --nocapture` to
//! reproduce known correctness gaps. These remain ignored so ordinary CI keeps
//! signalling regressions in implemented guarantees without normalizing a
//! known distributed-safety defect.

use chrono::Utc;
use pulpitum::{
    DurableTable, InMemoryArchiveStore, InMemoryDurableBucketStore, Record, TableDefinition,
    TableId, TimeRange,
};
use std::sync::Arc;

#[tokio::test]
async fn tables_with_overlapping_bucket_ids_are_isolated() {
    let store = Arc::new(InMemoryDurableBucketStore::default());
    let archive = Arc::new(InMemoryArchiveStore::default());
    let messages = DurableTable::with_definition(
        TableDefinition::chat_messages("messages", TableId::new("messages").unwrap()),
        store.clone(),
        archive.clone(),
    )
    .unwrap();
    let reactions = DurableTable::with_definition(
        TableDefinition::chat_messages("reactions", TableId::new("reactions").unwrap()),
        store,
        archive,
    )
    .unwrap();
    let timestamp = Utc::now();
    let reaction = Record {
        partition_key: "shared-channel".into(),
        event_time: timestamp,
        sort_key: "reaction-1".into(),
        value: b"thumbs-up".to_vec(),
    };

    reactions.append(reaction).await.unwrap();

    let rows = messages
        .query(
            "shared-channel",
            TimeRange {
                start: timestamp - chrono::Duration::seconds(1),
                end: timestamp + chrono::Duration::seconds(1),
            },
        )
        .await
        .unwrap();

    assert!(
        rows.is_empty(),
        "records written through one logical table must be invisible to another"
    );
}

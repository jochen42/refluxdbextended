use super::*;
use crate::paths::{SnapshotInfoFilePath, TableIndexConversionCompletedPath, TableIndexPath};
use crate::table_index_cache::TableIndexCacheConfig;
use crate::{DatabaseTables, ParquetFile, PersistedSnapshot, PersistedSnapshotVersion};
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{ParquetFileId, SerdeVecMap};
use influxdb3_shutdown::ShutdownManager;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_time::{MockProvider, Time};
use metric::Registry;
use object_store::{ObjectStore, memory::InMemory};

#[tokio::test]
async fn test_retention_handler_no_policies() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let registry = Arc::new(Registry::default());
    let catalog = Arc::new(
        Catalog::new(
            "test_host".to_string(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Arc::clone(&registry),
        )
        .await
        .unwrap(),
    );

    let table_index_cache = TableIndexCache::new(
        "test_host".to_string(),
        TableIndexCacheConfig::default(),
        object_store,
    );

    let handler = Arc::new(RetentionPeriodHandler::new(
        table_index_cache.clone(),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        Duration::from_secs(30),
        "test_host".to_string(),
    ));

    // Check retention with no policies configured
    handler.check_and_enforce_retention().await;
    // Should complete without errors
}

#[tokio::test]
async fn test_retention_handler_shutdown() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let registry = Arc::new(Registry::default());
    let catalog = Arc::new(
        Catalog::new(
            "test_host".to_string(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Arc::clone(&registry),
        )
        .await
        .unwrap(),
    );

    let table_index_cache = TableIndexCache::new(
        "test_host".to_string(),
        TableIndexCacheConfig::default(),
        object_store,
    );

    let handler = Arc::new(RetentionPeriodHandler::new(
        table_index_cache.clone(),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        Duration::from_millis(100), // Short interval for testing
        "test_host".to_string(),
    ));

    let shutdown_manager = ShutdownManager::new_testing();
    let shutdown_token = shutdown_manager.register();

    // Start the background task
    let task_handle = tokio::spawn(handler.background_task(shutdown_token));

    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown
    shutdown_manager.shutdown();

    // Task should complete
    task_handle.await.unwrap();
}

/// Without retention policies the check must still fold new snapshots into the table indices,
/// otherwise the backlog grows with uptime and the next startup converts all of it.
#[tokio::test]
async fn test_retention_handler_syncs_table_indices_without_policies() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let registry = Arc::new(Registry::default());
    let catalog = Arc::new(
        Catalog::new(
            "test_host".to_string(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Arc::clone(&registry),
        )
        .await
        .unwrap(),
    );

    let table_index_cache = TableIndexCache::new(
        "test_host".to_string(),
        TableIndexCacheConfig::default(),
        Arc::clone(&object_store),
    );
    // startup with no snapshots yet
    table_index_cache.initialize().await.unwrap();

    // a snapshot persisted while the server runs
    let (db_id, table_id) = (DbId::from(1), TableId::from(2));
    let mut tables = SerdeVecMap::new();
    tables.insert(
        table_id,
        vec![ParquetFile {
            id: ParquetFileId::from(0),
            path: "test_file_0.parquet".to_string(),
            size_bytes: 1000,
            row_count: 100,
            chunk_time: 0,
            min_time: 0,
            max_time: 1000,
        }],
    );
    let mut databases = SerdeVecMap::new();
    databases.insert(db_id, DatabaseTables { tables });
    let seq = SnapshotSequenceNumber::new(1);
    let snapshot = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(1),
        snapshot_sequence_number: seq,
        wal_file_sequence_number: WalFileSequenceNumber::new(1),
        catalog_sequence_number: CatalogSequenceNumber::new(1),
        databases,
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1000,
        row_count: 100,
        parquet_size_bytes: 1000,
        persisted_at: None,
    });
    object_store
        .put(
            SnapshotInfoFilePath::new("test_host", seq).as_ref(),
            serde_json::to_vec(&snapshot).unwrap().into(),
        )
        .await
        .unwrap();

    let handler = RetentionPeriodHandler::new(
        table_index_cache,
        catalog,
        Arc::clone(&time_provider) as _,
        Duration::from_secs(30),
        "test_host".to_string(),
    );
    handler.check_and_enforce_retention().await;

    let marker: serde_json::Value = serde_json::from_slice(
        &object_store
            .get(TableIndexConversionCompletedPath::new("test_host").as_ref())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(marker["last_sequence_number"], 1);
    object_store
        .head(TableIndexPath::new("test_host", db_id.get(), table_id.get()).as_ref())
        .await
        .expect("table index should exist after the periodic sync");
}

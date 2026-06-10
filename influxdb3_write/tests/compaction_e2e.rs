//! End-to-end regression test for the compaction service.
//!
//! Lives in `tests/` rather than alongside the unit tests because
//! `ParquetFileId::new` increments a global atomic counter, and other tests in
//! the crate (e.g. `persister::tests::persist_add_parquet_file_and_load_snapshot`)
//! assert on its exact value. Running this in its own test binary gives it a
//! fresh process and avoids cross-test interference.

use data_types::NamespaceName;
use futures_util::StreamExt;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_shutdown::ShutdownManager;
use influxdb3_wal::WalConfig;
use influxdb3_write::compaction::{CompactionConfig, CompactionService};
use influxdb3_write::paths::CompactionInfoFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
use influxdb3_write::{Bufferer, PersistedSnapshot, PersistedSnapshotVersion, Precision, WriteBuffer};
use iox_query::exec::Executor;
use iox_time::MockProvider;
use metric::Registry;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn compaction_publishes_and_replaces_files() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider: Arc<dyn iox_time::TimeProvider> = Arc::new(MockProvider::new(
        iox_time::Time::from_timestamp_nanos(0),
    ));
    let catalog = Arc::new(
        Catalog::new(
            "test-host",
            Arc::clone(&object_store),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        "test-host",
        Arc::clone(&time_provider),
        None,
    ));
    let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
        .await
        .unwrap();
    let distinct_cache = DistinctCacheProvider::new_from_catalog(
        Arc::clone(&time_provider),
        Arc::clone(&catalog),
    )
    .await
    .unwrap();
    let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
        persister: Arc::clone(&persister),
        catalog: Arc::clone(&catalog),
        last_cache,
        distinct_cache,
        time_provider: Arc::clone(&time_provider),
        executor: Arc::new(Executor::new_testing()),
        wal_config: WalConfig::test_config(),
        parquet_cache: None,
        metric_registry: Arc::new(Registry::default()),
        snapshotted_wal_files_to_keep: 10,
        query_file_limit: None,
        n_snapshots_to_load_on_start: std::num::NonZeroU64::new(1).unwrap(),
        shutdown: ShutdownManager::new_testing().register(),
        wal_replay_concurrency_limit: 1,
        parquet_snapshot_concurrency_limit: std::num::NonZeroUsize::new(1).unwrap(),
        shared_inventory: None,
    })
    .await
    .unwrap();

    let db_name = "testdb";
    let table_name = "testtable";
    catalog.create_database(db_name).await.unwrap();
    let db_id = catalog.db_name_to_id(db_name).unwrap();

    for (i, ts) in [10u64, 20, 30, 40, 50].iter().enumerate() {
        let lp = format!("{table_name} value={i} {ts}");
        write_buffer
            .write_lp(
                NamespaceName::new(db_name).unwrap(),
                &lp,
                iox_time::Time::from_timestamp_nanos(0),
                false,
                Precision::Nanosecond,
                false,
            )
            .await
            .unwrap();
        let _ = write_buffer.wal().force_flush_buffer().await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let table_id = catalog
        .db_schema(db_name)
        .unwrap()
        .table_definition(table_name)
        .unwrap()
        .table_id;

    let gen1_files = write_buffer.persisted_files().get_files(db_id, table_id);
    assert!(
        gen1_files.len() >= 2,
        "expected multiple gen1 files, got {}",
        gen1_files.len()
    );

    let mut gen_durations = HashMap::new();
    gen_durations.insert(1, Duration::from_nanos(1));
    gen_durations.insert(2, Duration::from_nanos(1));
    let compaction_service = CompactionService::new(
        CompactionConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            max_files_per_run: 10,
            min_files_for_compaction: 2,
            generation_durations: gen_durations,
            // Delete immediately so the test can assert deletion happened.
            delete_grace: Duration::ZERO,
            // Off for the test — no inventory wired.
            checkpoint_every_n_cycles: 0,
            claim_ttl: Duration::from_secs(60),
        },
        Arc::clone(&catalog),
        write_buffer.clone() as Arc<dyn WriteBuffer>,
        Arc::clone(&persister),
        Arc::new(Executor::new_testing()),
        Arc::clone(&object_store),
        Arc::clone(&time_provider),
        ShutdownManager::new_testing().register(),
    );

    let jobs = compaction_service.identify_compaction_jobs().await.unwrap();
    assert_eq!(jobs.len(), 1, "expected one compaction job");
    let job = jobs.into_iter().next().unwrap();
    let input_paths = job.files.iter().map(|f| f.path.clone()).collect::<Vec<_>>();
    let input_rows: u64 = job.files.iter().map(|f| f.row_count).sum();

    let result = compaction_service
        .execute_compaction_job(job)
        .await
        .unwrap();

    assert_eq!(result.compacted_files.len(), 1, "expected one output file");
    assert_eq!(
        result.total_rows_compacted, input_rows,
        "rows should be preserved"
    );

    let out_path = ObjPath::from(result.compacted_files[0].path.clone());
    let head = object_store
        .head(&out_path)
        .await
        .expect("output parquet missing");
    assert!(head.size > 0, "output parquet is empty");
    assert!(
        result.compacted_files[0].path.contains("/gen2/"),
        "output path should be in gen2: {}",
        result.compacted_files[0].path
    );

    let after = write_buffer.persisted_files().get_files(db_id, table_id);
    let after_paths: std::collections::HashSet<_> =
        after.iter().map(|f| f.path.clone()).collect();
    assert!(
        after_paths.contains(&result.compacted_files[0].path),
        "compacted file not in PersistedFiles"
    );
    for old in &input_paths {
        assert!(
            !after_paths.contains(old),
            "input file {} still in PersistedFiles after compaction",
            old
        );
    }

    let manifests = persister.load_compaction_snapshots().await.unwrap();
    assert_eq!(manifests.len(), 1, "expected one compaction manifest");
    let manifest = &manifests[0];
    assert_eq!(manifest.removed_files.len(), 1);
    let removed_tables = manifest.removed_files.get(&db_id).unwrap();
    let removed_files = removed_tables.tables.get(&table_id).unwrap();
    assert_eq!(removed_files.len(), input_paths.len());

    tokio::time::sleep(Duration::from_millis(200)).await;
    for old in &input_paths {
        let path = ObjPath::from(old.clone());
        assert!(
            object_store.head(&path).await.is_err(),
            "input parquet {} should have been deleted",
            old
        );
    }

    // Restart-replay: a fresh PersistedFiles built from saved WAL + compaction
    // manifests should match the post-compaction view.
    let wal_snapshots: Vec<PersistedSnapshot> = persister
        .load_snapshots(1000)
        .await
        .unwrap()
        .into_iter()
        .map(|psv| match psv {
            PersistedSnapshotVersion::V1(ps) => ps,
        })
        .collect();
    let comp_snapshots = persister.load_compaction_snapshots().await.unwrap();
    let mut combined = wal_snapshots;
    combined.extend(comp_snapshots);
    let replayed = PersistedFiles::new_from_persisted_snapshots(None, Arc::new(combined));
    let replayed_paths: std::collections::HashSet<_> = replayed
        .get_files(db_id, table_id)
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(replayed_paths.contains(&result.compacted_files[0].path));
    for old in &input_paths {
        assert!(
            !replayed_paths.contains(old),
            "replay still sees deleted input {}",
            old
        );
    }

    // Manifest is under {host}/compactions/.
    let dir = CompactionInfoFilePath::dir("test-host");
    let mut list = object_store.list(Some(dir.as_ref()));
    let mut count = 0;
    while let Some(item) = list.next().await {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 1);
}

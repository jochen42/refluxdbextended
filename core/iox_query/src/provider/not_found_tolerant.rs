//! A [`FileSource`] decorator that turns object-store *NotFound* errors into an
//! empty scan for that single file, instead of failing the whole query.
//!
//! Why this exists: on a multi-node querier, compaction is publish-then-delete —
//! it publishes the superseding (higher-gen) file, then deletes the inputs after
//! a grace period. A querier can still hold the stale input ref (most visibly
//! just after a restart, before the ref-validator's first pass has swept the
//! booted [`PersistedFiles`]). The stale ref's object is already gone, so the
//! parquet scan GETs a 404 and the **entire** Flight query fails — even though
//! the superseding file is present in the very same plan (folding adds the new
//! ref before removing the old, and the ReorgPlanner dedupes the overlap). So
//! skipping the one missing file yields a correct result.
//!
//! Scope is deliberately narrow: only `object_store::Error::NotFound` anywhere in
//! the error source chain is tolerated. Auth failures, corruption, and transient
//! errors still fail loudly so we never silently serve short results for a file
//! that actually exists.

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use datafusion::common::{Result, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::schema_adapter::SchemaAdapterFactory;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{FutureExt, StreamExt};
use object_store::ObjectStore;
use tracing::warn;

/// Walk the error source chain; `true` if any link is
/// [`object_store::Error::NotFound`]. The mem-cache layer may re-wrap a NotFound
/// inside a `Generic` error, so a typed walk (not a top-level `matches!`) is
/// required to catch it.
fn is_object_store_not_found(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(os) = e.downcast_ref::<object_store::Error>()
            && matches!(os, object_store::Error::NotFound { .. })
        {
            return true;
        }
        cur = e.source();
    }
    false
}

/// Wraps a [`FileOpener`]; a NotFound at open time becomes an empty stream.
struct NotFoundTolerantOpener {
    inner: Arc<dyn FileOpener>,
}

impl FileOpener for NotFoundTolerantOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        // A missing object can surface either at open (footer GET) or mid-stream
        // (row-group GET when the footer was already cached before the object was
        // deleted). Tolerate NotFound in both places: at open, yield an empty
        // stream; mid-stream, end this file early. The surviving (higher-gen)
        // file in the same plan supplies the rows.
        let path = file.object_meta.location.clone();
        let fut = self.inner.open(file)?;
        Ok(async move {
            match fut.await {
                Ok(stream) => {
                    let path = path.clone();
                    let mut warned = false;
                    let mapped = stream.scan(false, move |stopped, item| {
                        if *stopped {
                            return futures::future::ready(None);
                        }
                        match item {
                            Ok(batch) => futures::future::ready(Some(Ok(batch))),
                            Err(e) if is_object_store_not_found(&e) => {
                                if !warned {
                                    warn!(
                                        %path,
                                        "parquet object missing mid-scan; ending file \
                                         early (stale ref, compaction-deleted)"
                                    );
                                    warned = true;
                                }
                                *stopped = true;
                                futures::future::ready(None)
                            }
                            // Propagate any other error unchanged.
                            Err(e) => futures::future::ready(Some(Err(e))),
                        }
                    });
                    Ok(mapped.boxed())
                }
                Err(e) if is_object_store_not_found(&e) => {
                    warn!(
                        %path,
                        "parquet object missing at open; skipping file as empty \
                         (stale ref, likely compaction-deleted before ref validation)"
                    );
                    Ok(futures::stream::empty().boxed())
                }
                Err(e) => Err(e),
            }
        }
        .boxed())
    }
}

/// A [`FileSource`] that delegates entirely to `inner` but wraps every
/// [`FileOpener`] it produces in [`NotFoundTolerantOpener`].
///
/// All `with_*`/pushdown methods re-wrap their result so the tolerance survives
/// the planning rewrites DataFusion performs (projection, batch size, filter
/// pushdown, schema adapter) before `create_file_opener` is finally called.
pub struct NotFoundTolerantSource {
    inner: Arc<dyn FileSource>,
}

impl NotFoundTolerantSource {
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }

    fn rewrap(inner: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self { inner })
    }
}

impl Debug for NotFoundTolerantSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotFoundTolerantSource")
            .field("inner", &self.inner.file_type())
            .finish()
    }
}

impl FileSource for NotFoundTolerantSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(NotFoundTolerantOpener {
            inner: self
                .inner
                .create_file_opener(object_store, base_config, partition),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Self::rewrap(self.inner.with_batch_size(batch_size))
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        Self::rewrap(self.inner.with_schema(schema))
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Self::rewrap(self.inner.with_projection(config))
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Self::rewrap(self.inner.with_statistics(statistics))
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        // Returns a `FileScanConfig` whose `file_source` is `config`'s (this
        // wrapper), so tolerance is preserved without re-wrapping here.
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let mut prop = self.inner.try_pushdown_filters(filters, config)?;
        if let Some(updated) = prop.updated_node.take() {
            prop.updated_node = Some(Self::rewrap(updated));
        }
        Ok(prop)
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Self::rewrap(
            self.inner.with_schema_adapter_factory(factory)?,
        ))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::path::Path as ObjPath;

    #[test]
    fn detects_direct_not_found() {
        let e = object_store::Error::NotFound {
            path: "p".to_string(),
            source: "x".into(),
        };
        assert!(is_object_store_not_found(&e));
    }

    #[test]
    fn detects_not_found_nested_in_generic() {
        // mem-cache wraps a NotFound inside Generic.source — must still be caught.
        let inner = object_store::Error::NotFound {
            path: ObjPath::from("a/b.parquet").to_string(),
            source: "404".into(),
        };
        let wrapped = object_store::Error::Generic {
            store: "mem_cached_object_store",
            source: Box::new(inner),
        };
        let df = datafusion::error::DataFusionError::ObjectStore(Box::new(wrapped));
        assert!(is_object_store_not_found(&df));
    }

    #[test]
    fn ignores_other_errors() {
        let e = object_store::Error::Generic {
            store: "s",
            source: "boom".into(),
        };
        assert!(!is_object_store_not_found(&e));
        let df = datafusion::error::DataFusionError::External("nope".into());
        assert!(!is_object_store_not_found(&df));
    }

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::common::Result as DfResult;
    use datafusion::datasource::listing::PartitionedFile;
    use futures::StreamExt;
    use std::sync::Arc;

    fn one_row() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap()
    }

    fn not_found_err() -> datafusion::error::DataFusionError {
        datafusion::error::DataFusionError::ObjectStore(Box::new(object_store::Error::NotFound {
            path: "gone.parquet".to_string(),
            source: "404".into(),
        }))
    }

    /// A [`FileOpener`] whose stream yields the given items.
    struct FakeOpener(Vec<DfResult<RecordBatch>>);
    impl FileOpener for FakeOpener {
        fn open(&self, _f: PartitionedFile) -> Result<FileOpenFuture> {
            let items: Vec<DfResult<RecordBatch>> = self
                .0
                .iter()
                .map(|r| match r {
                    Ok(b) => Ok(b.clone()),
                    Err(_) => Err(not_found_err()),
                })
                .collect();
            Ok(async move { Ok(futures::stream::iter(items).boxed()) }.boxed())
        }
    }

    fn dummy_file() -> PartitionedFile {
        PartitionedFile::new("x.parquet".to_string(), 1)
    }

    async fn collect(opener: &NotFoundTolerantOpener) -> DfResult<Vec<RecordBatch>> {
        let stream = opener.open(dummy_file()).unwrap().await?;
        stream.collect::<Vec<_>>().await.into_iter().collect()
    }

    #[tokio::test]
    async fn mid_stream_not_found_ends_file_early() {
        // [row, NotFound, row] -> the file ends at the 404; the trailing row is
        // dropped (it doesn't exist — the surviving gen supplies those rows).
        // FakeOpener turns any `Err` item into an object-store NotFound.
        let inner = Arc::new(FakeOpener(vec![
            Ok(one_row()),
            Err(datafusion::error::DataFusionError::External("nf".into())),
            Ok(one_row()),
        ]));
        let opener = NotFoundTolerantOpener { inner };
        let batches = collect(&opener).await.expect("must not error on NotFound");
        assert_eq!(batches.len(), 1, "stops at the mid-stream NotFound");
    }

    #[tokio::test]
    async fn mid_stream_other_error_propagates() {
        struct OtherErrOpener;
        impl FileOpener for OtherErrOpener {
            fn open(&self, _f: PartitionedFile) -> Result<FileOpenFuture> {
                Ok(async move {
                    let items: Vec<DfResult<RecordBatch>> = vec![
                        Ok(one_row()),
                        Err(datafusion::error::DataFusionError::External("boom".into())),
                    ];
                    Ok(futures::stream::iter(items).boxed())
                }
                .boxed())
            }
        }
        let opener = NotFoundTolerantOpener {
            inner: Arc::new(OtherErrOpener),
        };
        let res = collect(&opener).await;
        assert!(res.is_err(), "non-NotFound errors still propagate");
    }
}

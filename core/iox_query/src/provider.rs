//! Implementation of a DataFusion `TableProvider` in terms of `QueryChunk`s

use async_trait::async_trait;
use std::{collections::HashSet, sync::Arc};

use arrow::datatypes::{Fields, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::common::{DFSchema, plan_datafusion_err};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, provider_as_source},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
        utils::{conjunction, split_conjunction},
    },
    physical_plan::{
        ExecutionPlan, expressions::col as physical_col, filter::FilterExec,
        projection::ProjectionExec,
    },
    prelude::Expr,
    sql::TableReference,
};
use schema::{Schema, sort::SortKey};
use tracing::trace;

use crate::{CHUNK_ORDER_COLUMN_NAME, QueryChunk, chunk_order_field, util::arrow_sort_key_exprs};

use snafu::{ResultExt, Snafu};

mod adapter;
mod deduplicate;
pub mod overlap;
mod physical;
pub(crate) mod progressive_eval;
mod record_batch_exec;
pub(crate) mod reorder_partitions;
pub use self::overlap::group_potential_duplicates;
pub use deduplicate::{DeduplicateExec, RecordBatchDeduplicator};
pub(crate) use physical::{PartitionedFileExt, chunks_to_physical_nodes};

pub(crate) use record_batch_exec::RecordBatchesExec;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal error: no chunk pruner provided to builder for {}",
        table_name,
    ))]
    InternalNoChunkPruner { table_name: String },

    #[snafu(display("Internal error: Cannot create projection select expr '{}'", source,))]
    InternalSelectExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding sort operator '{}'", source,))]
    InternalSort {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding filter operator '{}'", source,))]
    InternalFilter {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Internal error adding projection operator '{}'", source,))]
    InternalProjection {
        source: datafusion::error::DataFusionError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        match e {
            e @ Error::InternalNoChunkPruner { .. } => Self::External(Box::new(e)),
            Error::InternalSelectExpr { source }
            | Error::InternalSort { source }
            | Error::InternalFilter { source }
            | Error::InternalProjection { source } => source,
        }
    }
}

/// Builds a `ChunkTableProvider` from a series of `QueryChunk`s
/// and ensures the schema across the chunks is compatible and
/// consistent.
#[derive(Debug)]
pub struct ProviderBuilder {
    table_name: Arc<str>,
    schema: Schema,
    chunks: Vec<Arc<dyn QueryChunk>>,
    deduplication: bool,
}

impl ProviderBuilder {
    pub fn new(table_name: Arc<str>, schema: Schema) -> Self {
        assert_eq!(schema.find_index_of(CHUNK_ORDER_COLUMN_NAME), None);

        Self {
            table_name,
            schema,
            chunks: Vec::new(),
            deduplication: true,
        }
    }

    pub fn with_enable_deduplication(mut self, enable_deduplication: bool) -> Self {
        self.deduplication = enable_deduplication;
        self
    }

    /// Add a new chunk to this provider
    pub fn add_chunk(mut self, chunk: Arc<dyn QueryChunk>) -> Self {
        self.chunks.push(chunk);
        self
    }

    /// Create the Provider
    pub fn build(self) -> Result<ChunkTableProvider> {
        Ok(ChunkTableProvider {
            iox_schema: self.schema,
            table_name: self.table_name,
            chunks: self.chunks,
            deduplication: self.deduplication,
        })
    }
}

/// Implementation of a DataFusion TableProvider in terms of QueryChunks
///
/// This allows DataFusion to see data from Chunks as a single table, as well as
/// push predicates and selections down to chunks
#[derive(Debug, Clone)]
pub struct ChunkTableProvider {
    table_name: Arc<str>,
    /// The IOx schema (wrapper around Arrow Schemaref) for this table
    iox_schema: Schema,
    /// The chunks
    chunks: Vec<Arc<dyn QueryChunk>>,
    /// do deduplication
    deduplication: bool,
}

impl ChunkTableProvider {
    /// Return the IOx schema view for the data provided by this provider
    pub fn iox_schema(&self) -> &Schema {
        &self.iox_schema
    }

    /// Return the Arrow schema view for the data provided by this provider
    pub fn arrow_schema(&self) -> ArrowSchemaRef {
        self.iox_schema.as_arrow()
    }

    /// Return the table name
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    /// Running deduplication or not
    pub fn deduplication(&self) -> bool {
        self.deduplication
    }

    /// Convert into a logical plan builder.
    pub fn into_logical_plan_builder(
        self: Arc<Self>,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let table_name = self.table_name().to_owned();
        let source = provider_as_source(self as _);

        // Scan all columns (DataFusion optimizer will prune this
        // later if possible)
        let projection = None;

        // Do not parse the tablename as a SQL identifer, but use as is
        let table_ref = TableReference::bare(table_name);
        LogicalPlanBuilder::scan(table_ref, source, projection)
    }
}

#[async_trait]
impl TableProvider for ChunkTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Schema with all available columns across all chunks
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema()
    }

    /// Creates a plan like the following:
    ///
    /// ```text
    /// Project (keep only columns needed in the rest of the plan)
    ///   Filter (optional, apply any push down predicates)
    ///     Deduplicate (optional, if chunks overlap)
    ///       ... Scan of Chunks (RecordBatchExec / DataSourceExec / UnionExec, etc) ...
    /// ```
    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        trace!("Create a scan node for ChunkTableProvider");

        let schema_with_chunk_order = Arc::new(ArrowSchema::new(
            self.iox_schema
                .as_arrow()
                .fields
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ));
        let pk = self.iox_schema().primary_key();
        let dedup_sort_key = SortKey::from_columns(pk.iter().copied());

        // Create data stream from chunk data. This is the most simple data stream possible and contains duplicates and
        // has no filters at all.
        let plan = chunks_to_physical_nodes(
            &schema_with_chunk_order,
            None,
            self.chunks.clone(),
            ctx.config().target_partitions(),
        );

        // De-dup before doing anything else, because all logical expressions act on de-duplicated data.
        // NOTE: this wraps *all* chunks in a single `DeduplicateExec`; the
        // `SplitDedup` physical-optimizer rule later splits it per time-overlap
        // group and drops it entirely for non-overlapping chunks.
        let plan = if self.deduplication {
            let sort_exprs = arrow_sort_key_exprs(&dedup_sort_key, &plan.schema())
                .ok_or_else(|| plan_datafusion_err!("de-dup with empty sort key"))?;
            Arc::new(DeduplicateExec::new(plan, sort_exprs, true))
        } else {
            plan
        };

        // Filter as early as possible (AFTER de-dup!). Predicate pushdown will eventually push down parts of this.
        let plan = if let Some(expr) = filters.iter().cloned().reduce(|a, b| a.and(b)) {
            let maybe_expr = if !self.deduplication {
                let dedup_cols = pk.into_iter().collect::<HashSet<_>>();
                conjunction(
                    split_conjunction(&expr)
                        .into_iter()
                        .filter(|expr| {
                            expr.column_refs()
                                .into_iter()
                                .all(|c| dedup_cols.contains(c.name.as_str()))
                        })
                        .cloned(),
                )
            } else {
                Some(expr)
            };

            if let Some(expr) = maybe_expr {
                let df_schema = DFSchema::try_from(plan.schema())?;
                let filter_expr = ctx.create_physical_expr(expr, &df_schema)?;
                Arc::new(FilterExec::try_new(filter_expr, plan)?)
            } else {
                plan
            }
        } else {
            plan
        };

        // Project at last because it removes columns and hence other operations may fail. Projection pushdown will
        // optimize that later.
        // Always project because we MUST make sure that chunk order col doesn't leak to the user or to our parquet
        // files.
        let default_projection: Vec<_> = (0..self.iox_schema.len()).collect();
        let projection = projection.unwrap_or(&default_projection);
        let select_exprs = self
            .iox_schema()
            .select_by_indices(projection)
            .as_arrow()
            .fields()
            .iter()
            .map(|f| {
                let field_name = f.name();
                let physical_expr =
                    physical_col(field_name, &self.schema()).context(InternalSelectExprSnafu)?;
                Ok((physical_expr, field_name.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;

        let plan = Arc::new(ProjectionExec::try_new(select_exprs, plan)?);

        Ok(plan)
    }

    /// Filter pushdown specification
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        if self.deduplication {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        } else {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod test {
    use std::slice;

    use super::*;
    use crate::{
        exec::IOxSessionContext,
        pruning::retention_expr,
        test::{TestChunk, format_execution_plan},
    };
    use datafusion::prelude::{col, lit};

    #[tokio::test]
    async fn provider_scan_default() {
        let table_name = "t";
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .build()
            .unwrap();

        // simple plan
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        let expr = vec![lit(false)];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![TableProviderFilterPushDown::Exact]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    #[tokio::test]
    async fn provider_scan_no_dedup() {
        let table_name = "t";
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .with_enable_deduplication(false)
            .build()
            .unwrap();

        // simple plan
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), &[], None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        // Expressions on fields are NOT pushed down because they cannot be pushed through de-dup.
        let expr = vec![
            lit(false),
            col("tag1").eq(lit("foo")),
            col("field").eq(lit(1.0)),
        ];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact
            ]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND tag1@1 = CAST(foo AS Dictionary(Int32, Utf8))"
        - "     UnionExec"
        - "       RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "       DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   UnionExec"
        - "     RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "     DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    #[tokio::test]
    async fn provider_scan_retention() {
        let table_name = "t";
        let pred = retention_expr(100);
        let chunk1 = Arc::new(
            TestChunk::new(table_name)
                .with_id(1)
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let chunk2 = Arc::new(
            TestChunk::new(table_name)
                .with_id(2)
                .with_dummy_parquet_file()
                .with_tag_column("tag1")
                .with_tag_column("tag2")
                .with_f64_field_column("field")
                .with_time_column(),
        ) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().clone();

        let ctx = IOxSessionContext::with_testing();
        let state = ctx.inner().state();

        let provider = ProviderBuilder::new(Arc::from(table_name), schema)
            .add_chunk(Arc::clone(&chunk1))
            .add_chunk(Arc::clone(&chunk2))
            .build()
            .unwrap();

        // simple plan
        let plan = provider
            .scan(&state, None, slice::from_ref(&pred), None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // projection
        let plan = provider
            .scan(&state, Some(&vec![1, 3]), slice::from_ref(&pred), None)
            .await
            .unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[tag1@1 as tag1, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // filters
        let expr = vec![lit(false), pred.clone()];
        let expr_ref = expr.iter().collect::<Vec<_>>();
        assert_eq!(
            provider.supports_filters_pushdown(&expr_ref).unwrap(),
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact
            ]
        );
        let plan = provider.scan(&state, None, &expr, None).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: false AND time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );

        // limit pushdown is unimplemented at the moment
        let plan = provider.scan(&state, None, &[pred], Some(1)).await.unwrap();
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProjectionExec: expr=[field@0 as field, tag1@1 as tag1, tag2@2 as tag2, time@3 as time]"
        - "   FilterExec: time@3 > 100"
        - "     DeduplicateExec: [tag1@1 ASC,tag2@2 ASC,time@3 ASC]"
        - "       UnionExec"
        - "         RecordBatchesExec: chunks=1, projection=[field, tag1, tag2, time, __chunk_order]"
        - "         DataSourceExec: file_groups={1 group: [[2.parquet]]}, projection=[field, tag1, tag2, time, __chunk_order], output_ordering=[__chunk_order@4 ASC], file_type=parquet"
        "#
        );
    }

    /// The `SplitDedup` physical-optimizer rule must drop the `DeduplicateExec`
    /// entirely when chunks are time-disjoint (no two can share a primary key),
    /// yet keep it when some chunk's time range spans the others — exactly the
    /// shape an un-compacted backfill file (wide data-time range) or a chunk
    /// missing time-range stats produces. This pins the behaviour the slow
    /// wide-aggregation investigation hinges on.
    #[tokio::test]
    async fn split_dedup_drops_dedup_for_disjoint_chunks() {
        use crate::exec::Executor;
        use datafusion::catalog::TableProvider;

        // record-batch chunk over [tmin, tmax]; pass tmin/tmax = None to omit
        // time-range stats (which forces every chunk into one overlap group).
        fn make(id: u128, range: Option<(i64, i64)>) -> Arc<dyn QueryChunk> {
            let base = TestChunk::new("t")
                .with_id(id)
                .with_order(id as i64)
                .with_f64_field_column("field");
            let base = match range {
                Some((min, max)) => base.with_time_column_with_stats(Some(min), Some(max)),
                None => base.with_time_column(),
            };
            Arc::new(base) as Arc<dyn QueryChunk>
        }

        async fn dedup_nodes(executor: &Executor, chunks: Vec<Arc<dyn QueryChunk>>) -> usize {
            let ctx = executor.new_context();
            let provider = chunks
                .iter()
                .fold(
                    ProviderBuilder::new(Arc::from("t"), chunks[0].schema().clone()),
                    |b, c| b.add_chunk(Arc::clone(c)),
                )
                .build()
                .unwrap();
            ctx.inner()
                .register_table("t", Arc::new(provider) as Arc<dyn TableProvider>)
                .unwrap();
            let plan = ctx
                .inner()
                .sql("SELECT count(1), avg(field) FROM t")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            format_execution_plan(&plan)
                .iter()
                .filter(|l| l.contains("DeduplicateExec"))
                .count()
        }

        let executor = Executor::new_testing();

        // disjoint -> SplitDedup removes the dedup entirely
        let disjoint = vec![
            make(1, Some((0, 100))),
            make(2, Some((1_000, 1_100))),
            make(3, Some((2_000, 2_100))),
        ];
        assert_eq!(
            dedup_nodes(&executor, disjoint).await,
            0,
            "time-disjoint chunks must not be deduplicated"
        );

        // one chunk spanning all others (un-compacted backfill) -> dedup remains
        let backfill = vec![
            make(1, Some((0, 100))),
            make(2, Some((1_000, 1_100))),
            make(3, Some((2_000, 2_100))),
            make(4, Some((0, 2_100))),
        ];
        assert!(
            dedup_nodes(&executor, backfill).await >= 1,
            "a chunk spanning all others must force deduplication"
        );

        // a chunk missing time-range stats -> all lumped together -> dedup remains
        let no_stats = vec![
            make(1, Some((0, 100))),
            make(2, Some((1_000, 1_100))),
            make(3, None),
        ];
        assert!(
            dedup_nodes(&executor, no_stats).await >= 1,
            "a chunk without time-range stats must force deduplication"
        );
    }

    /// Declaring a chunk's `sort_key` (the order its parquet was written in) lets
    /// DataFusion satisfy the dedup's required ordering by *merging* the already
    /// sorted files (`SortPreservingMergeExec`) instead of fully re-sorting every
    /// row (`SortExec`). This is the perf fix for slow wide aggregations: the
    /// fork's `parquet_chunk_from_file` previously declared `sort_key: None`,
    /// forcing a full sort of millions of rows for de-duplication.
    #[tokio::test]
    async fn declared_sort_key_merges_instead_of_full_resort() {
        use crate::exec::Executor;
        use datafusion::catalog::TableProvider;

        // two parquet chunks, each sorted by [tag, time], OVERLAPPING in time so
        // a cross-file dedup is required.
        fn make(id: u128, tmin: i64, tmax: i64, declare_sort_key: bool) -> Arc<dyn QueryChunk> {
            let c = TestChunk::new("t")
                .with_id(id)
                .with_order(id as i64)
                .with_dummy_parquet_file()
                .with_tag_column("tag")
                .with_f64_field_column("field")
                .with_time_column_with_stats(Some(tmin), Some(tmax));
            let c = if declare_sort_key {
                c.with_sort_key(SortKey::from_columns(["tag", "time"]))
            } else {
                c
            };
            Arc::new(c) as Arc<dyn QueryChunk>
        }

        async fn count_execs(chunks: Vec<Arc<dyn QueryChunk>>) -> (usize, usize) {
            let executor = Executor::new_testing();
            let ctx = executor.new_context();
            let provider = chunks
                .iter()
                .fold(
                    ProviderBuilder::new(Arc::from("t"), chunks[0].schema().clone()),
                    |b, c| b.add_chunk(Arc::clone(c)),
                )
                .build()
                .unwrap();
            ctx.inner()
                .register_table("t", Arc::new(provider) as Arc<dyn TableProvider>)
                .unwrap();
            let plan = ctx
                .inner()
                .sql("SELECT tag, time, field FROM t")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let lines = format_execution_plan(&plan);
            // count *full* sorts (SortExec, not SortPreservingMergeExec)
            let full_sorts = lines
                .iter()
                .filter(|l| l.contains("SortExec"))
                .count();
            let merges = lines
                .iter()
                .filter(|l| l.contains("SortPreservingMergeExec"))
                .count();
            (full_sorts, merges)
        }

        let (sorts_declared, merges_declared) =
            count_execs(vec![make(1, 0, 100, true), make(2, 50, 150, true)]).await;
        let (sorts_none, _merges_none) =
            count_execs(vec![make(1, 0, 100, false), make(2, 50, 150, false)]).await;

        assert!(
            sorts_declared < sorts_none,
            "declared sort key should remove full re-sorts: declared={sorts_declared} none={sorts_none}"
        );
        assert!(
            merges_declared >= 1,
            "declared sort key should merge pre-sorted files (got {merges_declared} merges)"
        );
    }

    /// End-to-end timing comparison of the *existing* `SplitDedup` optimizer
    /// rule firing vs not. Both runs go through the real IOx planning + optimizer
    /// + executor (via `Executor::new_context`). The only difference is whether
    /// chunks carry time-range stats:
    ///
    /// * with stats, time-disjoint  -> `SplitDedup` splits, the historical
    ///   chunks skip dedup, and the giant global sort disappears (FAST);
    /// * without stats              -> `group_potential_duplicates` lumps every
    ///   chunk into one group, `SplitDedup` can't split, and all rows go through
    ///   one `DeduplicateExec` + sort (SLOW).
    ///
    /// This reproduces the production slow-aggregation: a single chunk whose
    /// time range spans everything (un-compacted backfill, or a chunk missing
    /// time stats) defeats `SplitDedup` and forces the whole scan through one
    /// sort. Models many time-disjoint compacted generations plus a small
    /// recent, overlapping write window.
    ///
    /// Ignored by default (timing + heavy); run with:
    ///   cargo test -p iox_query --lib bench_split_dedup -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn bench_split_dedup_fires_vs_defeated() {
        use crate::exec::Executor;
        use arrow::array::{ArrayRef, Float64Array, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, SchemaRef};
        use arrow::record_batch::RecordBatch;
        use datafusion::catalog::TableProvider;
        use std::time::Instant;

        const ROWS_PER_CHUNK: usize = 20_000;
        const N_HISTORICAL: usize = 40; // time-disjoint compacted generations
        const N_RECENT: usize = 4; // overlapping recent write window
        const DT: i64 = 1_000_000; // 1ms between points
        let span = ROWS_PER_CHUNK as i64 * DT;

        fn gen_batch(schema: &SchemaRef, n: usize, t0: i64, dt: i64) -> RecordBatch {
            let cols: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|f| {
                    if f.name() == "time" {
                        let times: Vec<i64> = (0..n as i64).map(|i| t0 + i * dt).collect();
                        let arr = TimestampNanosecondArray::from(times);
                        match f.data_type() {
                            DataType::Timestamp(_, Some(tz)) => {
                                Arc::new(arr.with_timezone(tz.clone())) as ArrayRef
                            }
                            _ => Arc::new(arr) as ArrayRef,
                        }
                    } else {
                        // Deterministic, identical across chunks so dedup of the
                        // overlapping recent rows leaves the average unchanged.
                        let vals: Vec<f64> = (0..n).map(|i| (i % 97) as f64 + 0.5).collect();
                        Arc::new(Float64Array::from(vals)) as ArrayRef
                    }
                })
                .collect();
            RecordBatch::try_new(Arc::clone(schema), cols).unwrap()
        }

        // Build identical data either with or without time-range stats. With
        // stats -> overlap-grouped (new). Without -> one group -> global (old).
        let make_chunk = |id: u128, order: i64, tmin: i64, with_stats: bool| -> Arc<dyn QueryChunk> {
            let tmax = tmin + (ROWS_PER_CHUNK as i64 - 1) * DT;
            let base = TestChunk::new("tbl").with_f64_field_column("field");
            let base = if with_stats {
                base.with_time_column_with_stats(Some(tmin), Some(tmax))
            } else {
                base.with_time_column()
            };
            let schema = base.schema().as_arrow();
            let batch = gen_batch(&schema, ROWS_PER_CHUNK, tmin, DT);
            Arc::new(
                base.with_record_batch(batch)
                    .with_id(id)
                    .with_order(order)
                    .with_row_count(ROWS_PER_CHUNK as u64),
            ) as Arc<dyn QueryChunk>
        };

        let recent_t0 = (N_HISTORICAL as i64 + 8) * span * 2; // well past history
        let build = |with_stats: bool| -> Vec<Arc<dyn QueryChunk>> {
            let mut v = Vec::with_capacity(N_HISTORICAL + N_RECENT);
            for k in 0..N_HISTORICAL {
                // disjoint: leave a full span gap between chunks
                v.push(make_chunk(k as u128, k as i64, (k as i64) * span * 2, with_stats));
            }
            for r in 0..N_RECENT {
                // all recent chunks cover the SAME range -> overlap + dup keys
                v.push(make_chunk(
                    (N_HISTORICAL + r) as u128,
                    (N_HISTORICAL + r) as i64,
                    recent_t0,
                    with_stats,
                ));
            }
            v
        };

        let executor = Executor::new_testing();
        let ctx = executor.new_context();
        let schema = build(true)[0].schema().clone();

        for (name, with_stats) in [("tbl_nosplit", false), ("tbl_split", true)] {
            let provider = ProviderBuilder::new(Arc::from("tbl"), schema.clone());
            let provider = build(with_stats)
                .into_iter()
                .fold(provider, |b, c| b.add_chunk(c))
                .build()
                .unwrap();
            ctx.inner()
                .register_table(name, Arc::new(provider) as Arc<dyn TableProvider>)
                .unwrap();
        }

        let run = |table: &'static str| {
            let ctx = &ctx;
            async move {
                let sql = format!("SELECT count(1) AS n, avg(field) AS a FROM {table}");
                let batches = ctx.inner().sql(&sql).await.unwrap().collect().await.unwrap();
                let b = &batches[0];
                let n = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0);
                let a = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0);
                (n, a)
            }
        };

        // Show the planned shapes.
        for table in ["tbl_nosplit", "tbl_split"] {
            let sql = format!("SELECT count(1) AS n, avg(field) AS a FROM {table}");
            let plan = ctx
                .inner()
                .sql(&sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let dedups = format_execution_plan(&plan)
                .iter()
                .filter(|l| l.contains("DeduplicateExec"))
                .count();
            println!("[{table}] DeduplicateExec nodes in plan: {dedups}");
        }

        // Warm up + correctness: identical results whether or not SplitDedup fires.
        let (n_old, a_old) = run("tbl_nosplit").await;
        let (n_new, a_new) = run("tbl_split").await;
        assert_eq!(n_old, n_new, "row counts must match (dedup correctness)");
        assert!(
            (a_old - a_new).abs() < 1e-9,
            "averages must match: nosplit={a_old} split={a_new}"
        );
        println!(
            "rows after dedup: {n_new} (from {} raw)",
            (N_HISTORICAL + N_RECENT) * ROWS_PER_CHUNK
        );

        const ITERS: u32 = 6;
        let time_it = |table: &'static str| {
            let ctx = &ctx;
            async move {
                let mut best = std::time::Duration::MAX;
                let mut total = std::time::Duration::ZERO;
                for _ in 0..ITERS {
                    let t = Instant::now();
                    let sql = format!("SELECT count(1) AS n, avg(field) AS a FROM {table}");
                    let _ = ctx.inner().sql(&sql).await.unwrap().collect().await.unwrap();
                    let e = t.elapsed();
                    best = best.min(e);
                    total += e;
                }
                (best, total / ITERS)
            }
        };

        let (old_best, old_avg) = time_it("tbl_nosplit").await;
        let (new_best, new_avg) = time_it("tbl_split").await;

        println!("\n=== SplitDedup defeated vs firing ===");
        println!(
            "chunks: {N_HISTORICAL} historical (disjoint) + {N_RECENT} recent (overlapping), {ROWS_PER_CHUNK} rows each"
        );
        println!("DEFEATED (no time stats -> 1 global dedup+sort): best={old_best:?} avg={old_avg:?}");
        println!("FIRING   (disjoint -> historical skip dedup):    best={new_best:?} avg={new_avg:?}");
        println!(
            "speedup: {:.2}x (best), {:.2}x (avg)",
            old_best.as_secs_f64() / new_best.as_secs_f64(),
            old_avg.as_secs_f64() / new_avg.as_secs_f64(),
        );
    }
}

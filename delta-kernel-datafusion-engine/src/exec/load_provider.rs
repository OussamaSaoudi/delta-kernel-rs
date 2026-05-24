//! Lazy [`TableProvider`] for `NodeKind::Load`: defers all work to `scan()`, which lowers
//! the upstream `LogicalPlan` and wraps it in a [`super::LoadExec`]. This PR only handles
//! the no-deletion-vector / no-column-mapping subset; DV plans are rejected at
//! [`super::LoadExec::new`] construction. Filter pushdown is currently off; projection and
//! limit flow through to [`super::LoadExec`].

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use delta_kernel::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::plans::ir::nodes::LoadNode;
use delta_kernel::schema::SchemaRef;
use delta_kernel::Engine;

use crate::error::plan_compilation;
use crate::exec::load_helpers::strip_nested_metadata_only;
use crate::exec::LoadExec;

/// Table provider that lowers a `NodeKind::Load` upstream into [`LoadExec`] at scan time.
pub struct LoadTableProvider {
    upstream_logical: LogicalPlan,
    node: Arc<LoadNode>,
    engine: Arc<dyn Engine>,
    /// `file_schema_fields ++ passthrough_fields`, pre-materialized so `schema()` is cheap.
    output_schema: ArrowSchemaRef,
}

impl LoadTableProvider {
    /// Construct from the `NodeKind::Load` payload plus the precomputed kernel-typed
    /// output schema. The caller (`lower_load`) computes `output_kernel_schema` by
    /// composing the load's `file_schema` with the per-passthrough-column types resolved
    /// against the upstream's kernel schema; this provider just converts it to arrow.
    pub fn try_new(
        upstream_logical: LogicalPlan,
        node: Arc<LoadNode>,
        engine: Arc<dyn Engine>,
        output_kernel_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        // Mirror the per-field metadata policy in [`super::LoadExec`]'s TableSchema (see
        // [`super::load_helpers::build_file_source`] for the rationale): file fields strip nested
        // metadata to match the parquet decoder's bare output; passthrough fields pass through
        // verbatim to match the partition-col broadcast.
        let file_field_count = node.file_schema.fields().len();
        let kernel_arrow_schema: ArrowSchema = output_kernel_schema
            .as_ref()
            .try_into_arrow()
            .map_err(|e| plan_compilation(format!("LoadTableProvider output schema: {e}")))?;
        let adjusted_fields: Vec<_> = kernel_arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i < file_field_count {
                    Arc::new(strip_nested_metadata_only(f.as_ref()))
                } else {
                    Arc::clone(f)
                }
            })
            .collect();
        let output_schema: ArrowSchemaRef = Arc::new(
            ArrowSchema::new(adjusted_fields).with_metadata(kernel_arrow_schema.metadata().clone()),
        );
        Ok(Self {
            upstream_logical,
            node,
            engine,
            output_schema,
        })
    }
}

impl std::fmt::Debug for LoadTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadTableProvider")
            .field("file_type", &self.node.file_type)
            .field("output_field_count", &self.output_schema.fields().len())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for LoadTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let upstream_physical = state.create_physical_plan(&self.upstream_logical).await?;
        let load_exec = LoadExec::new(
            upstream_physical,
            Arc::clone(&self.node),
            Arc::clone(&self.engine),
            Arc::clone(&self.output_schema),
            projection.cloned(),
            limit,
        )?;
        Ok(Arc::new(load_exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

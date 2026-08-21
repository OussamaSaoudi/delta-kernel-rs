//! Execution-time materialization for shared nodes in a kernel plan DAG.

use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion_common::arrow::datatypes::SchemaRef;
use datafusion_common::error::DataFusionError;
use datafusion_common::Result as DfResult;
use datafusion_execution::TaskContext;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use tokio::sync::OnceCell;

/// A table provider whose scans share one materialized execution of `input`.
pub(crate) struct SharedNodeTableProvider {
    input: LogicalPlan,
    schema: SchemaRef,
    cache: Arc<OnceCell<Vec<datafusion_common::arrow::record_batch::RecordBatch>>>,
    node_index: usize,
}

impl SharedNodeTableProvider {
    pub(crate) fn new(input: LogicalPlan, node_index: usize) -> Self {
        Self {
            schema: Arc::new(input.schema().as_arrow().clone()),
            input,
            cache: Arc::new(OnceCell::new()),
            node_index,
        }
    }
}

impl fmt::Debug for SharedNodeTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedNodeTableProvider")
            .field("node_index", &self.node_index)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SharedNodeTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let input = state.create_physical_plan(&self.input).await?;
        let projection = projection.cloned();
        Ok(Arc::new(SharedNodeExec::try_new(
            input,
            Arc::clone(&self.cache),
            projection,
            self.node_index,
        )?))
    }
}

struct SharedNodeExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<OnceCell<Vec<datafusion_common::arrow::record_batch::RecordBatch>>>,
    projection: Option<Vec<usize>>,
    node_index: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SharedNodeExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        cache: Arc<OnceCell<Vec<datafusion_common::arrow::record_batch::RecordBatch>>>,
        projection: Option<Vec<usize>>,
        node_index: usize,
    ) -> DfResult<Self> {
        let schema = match projection.as_ref() {
            Some(projection) => Arc::new(input.schema().project(projection)?),
            None => input.schema(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            cache,
            projection,
            node_index,
            schema,
            properties,
        })
    }
}

impl fmt::Debug for SharedNodeExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedNodeExec")
            .field("node_index", &self.node_index)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SharedNodeExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SharedNodeExec: node={}", self.node_index)
    }
}

impl ExecutionPlan for SharedNodeExec {
    fn name(&self) -> &str {
        "SharedNodeExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [input] = children.try_into().map_err(|children: Vec<_>| {
            DataFusionError::Plan(format!(
                "SharedNodeExec requires one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::try_new(
            input,
            Arc::clone(&self.cache),
            self.projection.clone(),
            self.node_index,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "SharedNodeExec has one partition, requested {partition}"
            )));
        }
        let cache = Arc::clone(&self.cache);
        let input = Arc::clone(&self.input);
        let projection = self.projection.clone();
        let stream = try_stream! {
            let batches = cache
                .get_or_try_init(|| async move { collect(input, context).await })
                .await?;
            for batch in batches {
                yield match projection.as_ref() {
                    Some(projection) => batch.project(projection)?,
                    None => batch.clone(),
                };
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion_common::arrow::array::Int64Array;
    use datafusion_common::arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::arrow::record_batch::RecordBatch;
    use datafusion_datasource::memory::MemorySourceConfig;

    use super::*;

    #[derive(Debug)]
    struct CountingExec {
        input: Arc<dyn ExecutionPlan>,
        executions: Arc<AtomicUsize>,
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(
            &self,
            _format: DisplayFormatType,
            formatter: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            write!(formatter, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &str {
            "CountingExec"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DfResult<Arc<dyn ExecutionPlan>> {
            let [input] = children.try_into().map_err(|children: Vec<_>| {
                DataFusionError::Plan(format!(
                    "CountingExec requires one child, got {}",
                    children.len()
                ))
            })?;
            Ok(Arc::new(Self {
                input,
                executions: Arc::clone(&self.executions),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> DfResult<SendableRecordBatchStream> {
            self.executions.fetch_add(1, Ordering::Relaxed);
            self.input.execute(partition, context)
        }
    }

    #[tokio::test]
    async fn shared_node_materializes_a_fanout_source_once() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("valid batch");
        let memory =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
        let executions = Arc::new(AtomicUsize::new(0));
        let input: Arc<dyn ExecutionPlan> = Arc::new(CountingExec {
            input: memory,
            executions: Arc::clone(&executions),
        });
        let cache = Arc::new(OnceCell::new());
        let first: Arc<dyn ExecutionPlan> = Arc::new(
            SharedNodeExec::try_new(Arc::clone(&input), Arc::clone(&cache), None, 0)
                .expect("shared node"),
        );
        let second: Arc<dyn ExecutionPlan> =
            Arc::new(SharedNodeExec::try_new(input, cache, None, 0).expect("shared node"));
        let context = Arc::new(TaskContext::default());

        let first_batches = collect(first, Arc::clone(&context))
            .await
            .expect("first read");
        let second_batches = collect(second, context).await.expect("second read");

        assert_eq!(first_batches[0].num_rows(), 3);
        assert_eq!(second_batches[0].num_rows(), 3);
        assert_eq!(executions.load(Ordering::Relaxed), 1);
    }
}

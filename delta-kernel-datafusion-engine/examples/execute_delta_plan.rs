//! Standard DataFusion CLI extended with Delta Kernel table support.

use std::any::Any;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{
    DynamicFileCatalog, Session, TableFunctionImpl, TableProvider, TableProviderFactory,
    UrlTableFactory,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::display::{PlanType, StringifiedPlan};
use datafusion::common::format::ExplainFormat;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext, SessionState};
use datafusion::logical_expr::{
    col, CreateExternalTable, Expr, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType, DisplayableExecutionPlan};
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, SendableRecordBatchStream};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_cli::exec::exec_from_repl;
use datafusion_cli::object_storage::instrumented::InstrumentedObjectStoreRegistry;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use datafusion_functions::core::expr_fn::get_field;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::scan::PartitionValuesOptions;
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use futures::{stream, TryStreamExt};
use url::Url;

const PLAN_DETAIL_WIDTH: usize = 72;

#[derive(Debug, Default)]
struct DeltaTableFactory;

#[derive(Debug, Default)]
struct DeltaUrlTableFactory;

#[derive(Debug)]
struct DeltaTableProvider {
    inner: Arc<dyn TableProvider>,
}

#[derive(Debug)]
struct DeltaCliQueryPlanner;

#[derive(Debug, Clone, Copy)]
enum DeltaRelationKind {
    Data,
    Metadata,
}

#[derive(Debug)]
struct DeltaTableFunction {
    name: &'static str,
    relation_kind: DeltaRelationKind,
}

#[derive(Debug)]
struct TreeAnalyzeExec {
    inner: Arc<AnalyzeExec>,
}

impl DisplayAs for TreeAnalyzeExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "TreeAnalyzeExec")
    }
}

impl ExecutionPlan for TreeAnalyzeExec {
    fn name(&self) -> &'static str {
        "TreeAnalyzeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "TreeAnalyzeExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "TreeAnalyzeExec expected partition 0, got {partition}"
            )));
        }

        let mut input = self.inner.execute(partition, context)?;
        let analyzed_plan = Arc::clone(self.inner.input());
        let schema = self.schema();
        let output_schema = Arc::clone(&schema);
        let output = async move {
            while input.try_next().await?.is_some() {}
            let plan = format_execution_tree(analyzed_plan.as_ref());
            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(vec!["Plan with Metrics"])),
                Arc::new(StringArray::from(vec![plan])),
            ];
            RecordBatch::try_new(output_schema, columns).map_err(DataFusionError::from)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(output),
        )))
    }
}

#[async_trait]
impl QueryPlanner for DeltaCliQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if let datafusion::logical_expr::LogicalPlan::Explain(explain) = logical_plan {
            let inner = DefaultPhysicalPlanner::default()
                .create_physical_plan(explain.plan.as_ref(), session_state)
                .await?;
            let plans = vec![StringifiedPlan::new(
                PlanType::FinalPhysicalPlan,
                format_execution_tree(inner.as_ref()),
            )];
            return Ok(Arc::new(ExplainExec::new(
                Arc::clone(explain.schema.inner()),
                plans,
                false,
            )));
        }
        let plan = DefaultPhysicalPlanner::default()
            .create_physical_plan(logical_plan, session_state)
            .await?;
        let Some(analyze) = plan.as_any().downcast_ref::<AnalyzeExec>() else {
            return Ok(plan);
        };
        Ok(Arc::new(TreeAnalyzeExec {
            inner: Arc::new(analyze.clone()),
        }))
    }
}

fn format_execution_tree(plan: &dyn ExecutionPlan) -> String {
    let mut output = String::new();
    format_execution_node(plan, "", true, true, &mut output);
    output
}

fn format_execution_node(
    plan: &dyn ExecutionPlan,
    prefix: &str,
    is_last: bool,
    is_root: bool,
    output: &mut String,
) {
    if !is_root {
        output.push_str(prefix);
        output.push_str(if is_last { "└─ " } else { "├─ " });
    }
    output.push_str(&format_execution_node_label(plan));
    output.push('\n');

    let children = plan.children();
    let child_prefix = if is_root {
        String::new()
    } else {
        format!("{prefix}{}", if is_last { "   " } else { "│  " })
    };
    let last_child = children.len().saturating_sub(1);
    for (index, child) in children.into_iter().enumerate() {
        format_execution_node(
            child.as_ref(),
            &child_prefix,
            index == last_child,
            false,
            output,
        );
    }
}

fn format_execution_node_label(plan: &dyn ExecutionPlan) -> String {
    let full_detail = DisplayableExecutionPlan::new(plan).one_line().to_string();
    let full_detail = full_detail.trim_end();
    let full_detail = if plan.name() == "ProjectionExec" {
        plan.name()
    } else {
        full_detail
    };
    let was_truncated = full_detail.chars().count() > PLAN_DETAIL_WIDTH;
    let mut detail = full_detail
        .chars()
        .take(PLAN_DETAIL_WIDTH)
        .collect::<String>();
    if was_truncated {
        detail.push_str("...");
    }

    let Some(metrics) = plan.metrics() else {
        return detail;
    };
    let mut values = Vec::new();
    if let Some(rows) = metrics.output_rows() {
        values.push(format!("rows={rows}"));
    }
    if let Some(nanos) = metrics.elapsed_compute() {
        values.push(format!("cpu={:?}", Duration::from_nanos(nanos as u64)));
    }
    if let Some(spills) = metrics.spill_count().filter(|spills| *spills > 0) {
        values.push(format!("spills={spills}"));
    }
    if values.is_empty() {
        detail
    } else {
        format!("{detail} [{}]", values.join(", "))
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        command: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let location = command.location.clone();
        tokio::task::spawn_blocking(move || open_delta_table(&location))
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!("Delta table loader task failed: {error}"))
            })?
    }
}

#[async_trait]
impl UrlTableFactory for DeltaUrlTableFactory {
    async fn try_new(&self, location: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if !location.contains('/') && Url::parse(location).is_err() {
            return Ok(None);
        }
        let location = location.to_string();
        let provider = tokio::task::spawn_blocking(move || {
            open_delta_relation(&location, DeltaRelationKind::Data)
        })
        .await
        .map_err(|error| {
            DataFusionError::Execution(format!("Delta URL-table loader task failed: {error}"))
        })??;
        Ok(Some(provider))
    }
}

impl TableFunctionImpl for DeltaTableFunction {
    fn call(&self, expressions: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let location = parse_table_function_location(self.name, expressions)?;
        let relation_kind = self.relation_kind;
        std::thread::spawn(move || open_delta_relation(&location, relation_kind))
            .join()
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "{} loader thread terminated unexpectedly",
                    self.name
                ))
            })?
    }
}

fn parse_table_function_location(name: &str, expressions: &[Expr]) -> DataFusionResult<String> {
    let [expression] = expressions else {
        return Err(DataFusionError::Plan(format!(
            "{name} requires exactly one string-literal table location"
        )));
    };
    match expression {
        Expr::Literal(ScalarValue::Utf8(Some(location)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(location)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(location)), _) => Ok(location.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "{name} requires a string-literal table location, for example \
             {name}('kernel/tests/data/basic_partitioned')"
        ))),
    }
}

fn parse_table_location(location: &str) -> DataFusionResult<Url> {
    if let Ok(url) = Url::parse(location) {
        if url.scheme() != "file" {
            return Err(DataFusionError::NotImplemented(format!(
                "Delta CLI demo supports local file URLs, not `{}`",
                url.scheme()
            )));
        }
        return Ok(url);
    }

    let path = Path::new(location).canonicalize().map_err(|error| {
        DataFusionError::Execution(format!(
            "cannot resolve Delta table location `{location}`: {error}"
        ))
    })?;
    Url::from_directory_path(&path).map_err(|()| {
        DataFusionError::Execution(format!(
            "cannot convert Delta table location `{}` to a file URL",
            path.display()
        ))
    })
}

fn open_delta_table(location: &str) -> DataFusionResult<Arc<dyn TableProvider>> {
    open_delta_relation(location, DeltaRelationKind::Data)
}

fn open_delta_relation(
    location: &str,
    relation_kind: DeltaRelationKind,
) -> DataFusionResult<Arc<dyn TableProvider>> {
    let table_url = parse_table_location(location)?;
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let scan = snapshot
        .scan_builder()
        .with_partition_values(PartitionValuesOptions::with_struct())
        .build()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let inner: Arc<dyn TableProvider> = match relation_kind {
        DeltaRelationKind::Metadata => {
            let executor = DataFusionExecutor::try_new_with_engine(Arc::clone(&engine))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let dataframe = executor
                .scan_metadata(&scan)
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .ok_or_else(|| {
                    DataFusionError::Execution("Delta metadata scan is empty".to_string())
                })?;
            let add = dataframe
                .schema()
                .field_with_unqualified_name("add")
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
            let datafusion::arrow::datatypes::DataType::Struct(fields) = add.data_type() else {
                return Err(DataFusionError::Plan(
                    "Delta metadata plan did not return an add struct".to_string(),
                ));
            };
            let projection = fields
                .iter()
                .map(|field| get_field(col("add"), field.name().to_string()).alias(field.name()))
                .collect::<Vec<_>>();
            dataframe.select(projection)?.into_view()
        }
        DeltaRelationKind::Data => {
            let batches = scan
                .execute(Arc::clone(&engine))
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .map(EngineDataArrowExt::try_into_record_batch)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let schema = if let Some(batch) = batches.first() {
                batch.schema()
            } else {
                let arrow: datafusion::arrow::datatypes::Schema = scan
                    .logical_schema()
                    .as_ref()
                    .try_into_arrow()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                Arc::new(arrow)
            };
            Arc::new(MemTable::try_new(schema, vec![batches])?)
        }
    };
    Ok(Arc::new(DeltaTableProvider { inner }) as Arc<dyn TableProvider>)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config_options = ConfigOptions::new();
    config_options.explain.format = ExplainFormat::Indent;
    config_options.explain.tree_maximum_render_width = 120;
    config_options.optimizer.enable_leaf_expression_pushdown = false;
    let config = SessionConfig::from(config_options).with_information_schema(true);
    let mut state = SessionContext::new_with_config(config)
        .into_state_builder()
        .with_query_planner(Arc::new(DeltaCliQueryPlanner))
        .build();
    state
        .table_factories_mut()
        .insert("DELTA".to_string(), Arc::new(DeltaTableFactory));
    let dynamic_catalog = Arc::new(DynamicFileCatalog::new(
        Arc::clone(state.catalog_list()),
        Arc::new(DeltaUrlTableFactory),
    ));
    state.register_catalog_list(dynamic_catalog);
    let session = SessionContext::new_with_state(state);
    session.register_udtf(
        "delta_scan",
        Arc::new(DeltaTableFunction {
            name: "delta_scan",
            relation_kind: DeltaRelationKind::Data,
        }),
    );
    session.register_udtf(
        "delta_table",
        Arc::new(DeltaTableFunction {
            name: "delta_table",
            relation_kind: DeltaRelationKind::Data,
        }),
    );
    session.register_udtf(
        "delta_metadata",
        Arc::new(DeltaTableFunction {
            name: "delta_metadata",
            relation_kind: DeltaRelationKind::Metadata,
        }),
    );

    let mut print_options = PrintOptions {
        format: PrintFormat::Table,
        quiet: false,
        maxrows: MaxRows::Limited(40),
        color: true,
        instrumented_registry: Arc::new(InstrumentedObjectStoreRegistry::default()),
    };

    println!("DataFusion CLI with Delta Kernel table support");
    println!("Query Delta tables directly with:");
    println!("  SELECT * FROM 'kernel/tests/data/table-without-dv-small';");
    println!(
        "  SELECT path, size FROM \
         delta_metadata('kernel/tests/data/table-without-dv-small');"
    );
    println!(
        "  EXPLAIN ANALYZE SELECT * FROM \
         delta_scan('kernel/tests/data/table-without-dv-small');"
    );
    println!("Run the complete demo with: \\i delta-kernel-datafusion-engine/examples/demo.sql");
    exec_from_repl(&session, &mut print_options).await?;
    Ok(())
}

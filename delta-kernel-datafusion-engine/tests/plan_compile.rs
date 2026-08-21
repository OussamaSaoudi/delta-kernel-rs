use std::sync::Arc;

use delta_kernel::arrow::array::AsArray;
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::expressions::{ColumnName, Scalar};
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::nodes::{Agg, Aggregate, Operator, Values};
use delta_kernel::plans::ir::plan::{Plan, PlanNode};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::{Engine, Snapshot};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use url::Url;

fn schema(fields: &[&str]) -> Arc<StructType> {
    Arc::new(
        StructType::try_new(
            fields
                .iter()
                .map(|name| StructField::nullable(*name, DataType::LONG)),
        )
        .expect("valid schema"),
    )
}

#[tokio::test]
async fn max_non_null_by_uses_aggregate_and_keeps_latest_row() {
    let input_schema = schema(&["key", "add", "version"]);
    let values = Values::new(
        Arc::clone(&input_schema),
        vec![
            vec![Scalar::Long(1), Scalar::Long(10), Scalar::Long(1)],
            vec![Scalar::Long(1), Scalar::Long(20), Scalar::Long(3)],
            vec![Scalar::Long(1), Scalar::Long(15), Scalar::Long(2)],
            vec![Scalar::Long(2), Scalar::Long(40), Scalar::Long(4)],
        ],
    );
    let aggregate = Aggregate::group_by(input_schema, [ColumnName::new(["key"])])
        .aggregate_as(
            Agg::max_non_null_by(
                ColumnName::new(["add"]),
                ColumnName::new(["add"]),
                ColumnName::new(["version"]),
            ),
            "add",
        )
        .build()
        .expect("valid aggregate");
    let plan = Plan {
        nodes: vec![
            PlanNode::new(Operator::Values(values), vec![]),
            PlanNode::new(Operator::Aggregate(aggregate), vec![0]),
        ],
    };

    let executor = DataFusionExecutor::try_new().expect("executor");
    let dataframe = executor.plan_to_dataframe(&plan).expect("dataframe");
    let physical = dataframe
        .clone()
        .create_physical_plan()
        .await
        .expect("physical plan");
    let display = datafusion_physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();
    assert!(display.contains("AggregateExec"), "{display}");
    assert!(!display.contains("SortExec"), "{display}");

    let batches = dataframe.collect().await.expect("collect");
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column_by_name("key")
            .expect("key")
            .as_primitive::<Int64Type>();
        let adds = batch
            .column_by_name("add")
            .expect("add")
            .as_primitive::<Int64Type>();
        rows.extend((0..batch.num_rows()).map(|row| (keys.value(row), adds.value(row))));
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 20), (2, 40)]);
}

#[tokio::test]
async fn static_json_scan_uses_native_datafusion_source() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data/basic_partitioned")
        .canonicalize()
        .expect("fixture path");
    let table_url = Url::from_directory_path(path).expect("table URL");
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot");
    let scan = snapshot.scan_builder().build().expect("scan");
    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let dataframe = executor
        .scan_metadata(&scan)
        .expect("metadata plan")
        .expect("non-empty metadata plan");
    let physical = dataframe
        .clone()
        .create_physical_plan()
        .await
        .expect("physical plan");
    let display = datafusion_physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();

    assert!(display.contains("DataSourceExec"), "{display}");
    assert!(!display.contains("LoadExec(file_type=Json"), "{display}");
    let row_count = dataframe
        .collect()
        .await
        .expect("metadata rows")
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(row_count, 6);
}

#[tokio::test]
async fn static_parquet_scan_uses_native_datafusion_source() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../kernel/tests/data/v1-single-part-struct-stats-only")
        .canonicalize()
        .expect("fixture path");
    let table_url = Url::from_directory_path(path).expect("table URL");
    let engine: Arc<dyn Engine> =
        Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build());
    let snapshot = Snapshot::builder_for(table_url)
        .build(engine.as_ref())
        .expect("snapshot");
    let scan = snapshot.scan_builder().build().expect("scan");
    let executor = DataFusionExecutor::try_new_with_engine(engine).expect("executor");
    let dataframe = executor
        .scan_metadata(&scan)
        .expect("metadata plan")
        .expect("non-empty metadata plan");
    let physical = dataframe
        .clone()
        .create_physical_plan()
        .await
        .expect("physical plan");
    let display = datafusion_physical_plan::displayable(physical.as_ref())
        .indent(false)
        .to_string();

    assert!(display.contains("DataSourceExec"), "{display}");
    assert!(!display.contains("LoadExec(file_type=Parquet"), "{display}");
    let row_count = dataframe
        .collect()
        .await
        .expect("metadata rows")
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(row_count, 5);
}

//! Integration tests for declarative `Window` (`row_number`) compilation and execution.

use std::sync::Arc;

use delta_kernel::arrow::array::{AsArray, RecordBatch};
use delta_kernel::arrow::datatypes::Int64Type;
use delta_kernel::expressions::{Expression, Scalar};
use delta_kernel::plans::ir::nodes::{OrderingSpec, WindowFunction};
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::DataFusionExecutor;

fn rn_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let schema = batch.schema();
    let idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .unwrap_or_else(|| panic!("column {name} not in schema {:?}", schema.fields()));
    batch
        .column(idx)
        .as_primitive::<Int64Type>()
        .values()
        .iter()
        .copied()
        .collect()
}

fn sample_schema() -> Arc<StructType> {
    Arc::new(
        StructType::try_new(vec![
            StructField::new("part", DataType::LONG, true),
            StructField::new("v", DataType::LONG, false),
        ])
        .expect("schema"),
    )
}

#[test]
fn row_number_resets_on_partition_change_stream_order() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
        vec![Scalar::Long(2), Scalar::Long(30)],
        vec![Scalar::Long(2), Scalar::Long(40)],
    ];
    let plan = DeclarativePlanNode::literal(schema.clone(), rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                function_name: "row_number".into(),
                args: vec![],
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![],
        )
        .results();

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches =
        futures::executor::block_on(async { exec.execute_plan_collect(plan).await }).expect("run");

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(rn_column(batch, "_rn"), vec![1, 2, 1, 2]);
}

#[test]
fn row_number_global_when_no_partition_keys() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(99), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::literal(schema, rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                function_name: "ROW_NUMBER".into(),
                args: vec![],
                output_col: "rn".into(),
            }],
            vec![],
            vec![],
        )
        .results();

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches =
        futures::executor::block_on(async { exec.execute_plan_collect(plan).await }).expect("run");

    assert_eq!(rn_column(&batches[0], "rn"), vec![1, 2]);
}

#[test]
fn multiple_row_number_functions_duplicate_rank_column() {
    let schema = sample_schema();
    let rows = vec![
        vec![Scalar::Long(1), Scalar::Long(10)],
        vec![Scalar::Long(1), Scalar::Long(20)],
    ];
    let plan = DeclarativePlanNode::literal(schema, rows)
        .expect("literal")
        .window(
            vec![
                WindowFunction {
                    function_name: "row_number".into(),
                    args: vec![],
                    output_col: "a".into(),
                },
                WindowFunction {
                    function_name: "row_number".into(),
                    args: vec![],
                    output_col: "b".into(),
                },
            ],
            vec![Arc::new(Expression::column(["part"]))],
            vec![],
        )
        .results();

    let exec = DataFusionExecutor::try_new().expect("executor");
    let batches =
        futures::executor::block_on(async { exec.execute_plan_collect(plan).await }).expect("run");

    assert_eq!(rn_column(&batches[0], "a"), vec![1, 2]);
    assert_eq!(rn_column(&batches[0], "b"), vec![1, 2]);
}

#[test]
fn window_with_order_by_is_rejected_at_compile_time() {
    let schema = sample_schema();
    let rows = vec![vec![Scalar::Long(1), Scalar::Long(10)]];
    let plan = DeclarativePlanNode::literal(schema, rows)
        .expect("literal")
        .window(
            vec![WindowFunction {
                function_name: "row_number".into(),
                args: vec![],
                output_col: "_rn".into(),
            }],
            vec![Arc::new(Expression::column(["part"]))],
            vec![OrderingSpec::asc(
                delta_kernel::expressions::ColumnName::new(["v"]),
            )],
        )
        .results();

    let exec = DataFusionExecutor::try_new().expect("executor");
    let err = exec.compile_plan(&plan).expect_err("order_by unsupported");
    let msg = format!("{err}");
    assert!(
        msg.contains("ORDER BY") || msg.contains("order_by"),
        "unexpected error: {msg}"
    );
}

//! Phase 3.4 insert SM smoke test over the DataFusion executor.

use std::sync::Arc;

use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::WriteSink;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::DataFusionExecutor;
use url::Url;

fn number_schema() -> delta_kernel::schema::SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "n",
        DataType::LONG,
    )]))
}

#[tokio::test]
async fn df_insert_sm_writes_parquet_for_literal_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ins.parquet");
    let dest = Url::from_file_path(&path).unwrap();

    let schema = number_schema();
    let rows = vec![
        vec![Scalar::Long(1)],
        vec![Scalar::Long(2)],
        vec![Scalar::Long(3)],
    ];
    let plan = DeclarativePlanNode::values(schema.clone(), rows)
        .unwrap()
        .into_write(WriteSink::parquet(dest));

    let ex = DataFusionExecutor::try_new().unwrap();
    ex.drive_insert_write_sm(plan).await.unwrap();

    assert!(path.exists(), "parquet artifact present");
}

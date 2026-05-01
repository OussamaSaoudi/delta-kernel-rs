//! Phase 3.4 insert SM + Phase 3.5 commit-action emission SM over the DataFusion executor.

use std::sync::Arc;

use delta_kernel::expressions::Scalar;
use delta_kernel::plans::ir::nodes::WriteSink;
use delta_kernel::plans::ir::DeclarativePlanNode;
use delta_kernel::plans::state_machines::df::{
    commit_action_emit_sm, commit_action_envelopes_literal,
};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::{DataFusionExecutor, DriveOpts};
use serde_json::Value;
use url::Url;

fn number_schema() -> delta_kernel::schema::SchemaRef {
    Arc::new(StructType::new_unchecked([StructField::not_null(
        "n",
        DataType::LONG,
    )]))
}

#[tokio::test]
async fn df_insert_sm_writes_parquet_and_row_count_matches_literal_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ins.parquet");
    let dest = Url::from_file_path(&path).unwrap();

    let schema = number_schema();
    let rows = vec![
        vec![Scalar::Long(1)],
        vec![Scalar::Long(2)],
        vec![Scalar::Long(3)],
    ];
    let plan = DeclarativePlanNode::literal(schema.clone(), rows)
        .unwrap()
        .into_write(WriteSink::parquet(dest));

    let ex = DataFusionExecutor::try_new().unwrap();
    let n = ex.drive_insert_write_sm(plan).await.unwrap();

    assert_eq!(n, 3);
    assert!(path.exists(), "parquet artifact present");
}

#[tokio::test]
async fn df_commit_emit_sm_returns_ordered_json_action_envelopes() {
    let commit = r#"{"commitInfo":{"operation":"WRITE","engineInfo":"kernel-test"}}"#;
    let add = r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":42}}"#;

    let (plan, extractor) =
        commit_action_envelopes_literal(vec![commit.into(), add.into()]).unwrap();
    let sm = commit_action_emit_sm(plan, extractor).unwrap();
    let ex = DataFusionExecutor::try_new().unwrap();
    let out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .unwrap();

    assert_eq!(out.len(), 2);
    let v0: Value = serde_json::from_str(&out[0]).unwrap();
    assert!(v0.get("commitInfo").is_some(), "{v0}");
    let v1: Value = serde_json::from_str(&out[1]).unwrap();
    assert!(v1.get("add").is_some(), "{v1}");
}

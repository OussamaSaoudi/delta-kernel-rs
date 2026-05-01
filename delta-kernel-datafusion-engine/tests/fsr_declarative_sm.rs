//! FSR declarative scaffold: kernel
//! [`CoroutineSM`](delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM)
//! bodies in [`delta_kernel::plans::state_machines::fsr`] driven by
//! [`DataFusionExecutor::drive_coroutine_sm`].

use std::fs::File;
use std::sync::Arc;

use delta_kernel::arrow::array::Int64Array;
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use delta_kernel::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use delta_kernel::expressions::Scalar;
use delta_kernel::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use delta_kernel::plans::state_machines::framework::state_machine::{AdvanceResult, StateMachine};
use delta_kernel::plans::state_machines::fsr::{
    try_build_fsr_footer_schema_sm, try_build_fsr_strip_then_fanout_sm, FsrFooterSchemaOutcome,
    FsrStripThenFanoutOutcome,
};
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel_datafusion_engine::{DataFusionExecutor, DriveOpts};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;
use url::Url;

fn long_schema() -> Arc<StructType> {
    Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap())
}

#[tokio::test]
async fn fsr_drive_two_phase_row_count_outcome() {
    let schema = long_schema();
    let sm = try_build_fsr_strip_then_fanout_sm(
        Arc::clone(&schema),
        vec![vec![Scalar::Long(1)], vec![Scalar::Long(2)]],
        schema,
        vec![
            vec![Scalar::Long(10)],
            vec![Scalar::Long(11)],
            vec![Scalar::Long(12)],
        ],
    )
    .expect("build SM");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive to completion");

    assert_eq!(
        out,
        FsrStripThenFanoutOutcome {
            strip_rows: 2,
            fanout_rows: 3,
        }
    );
}

#[tokio::test]
async fn fsr_phase_names_follow_strip_then_fanout_sequence() {
    let schema = long_schema();
    let mut sm: CoroutineSM<FsrStripThenFanoutOutcome> = try_build_fsr_strip_then_fanout_sm(
        Arc::clone(&schema),
        vec![vec![Scalar::Long(0)]],
        schema,
        vec![vec![Scalar::Long(1)]],
    )
    .expect("build SM");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let mut seen = Vec::new();
    loop {
        seen.push(sm.phase_name().to_string());
        let op = sm.get_operation().expect("op");
        let kdf = ex.execute_phase_operation(op).await.expect("phase exec");
        match sm.advance(Ok(kdf)).expect("advance") {
            AdvanceResult::Continue => {}
            AdvanceResult::Done(_) => break,
        }
    }

    assert_eq!(
        seen,
        vec![
            "fsr_strip_checkpoint_manifest".to_string(),
            "fsr_parallel_file_discovery".to_string(),
        ]
    );
}

#[tokio::test]
async fn fsr_footer_schema_coroutine_via_schema_query_phase() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("fsr.parquet");
    let arrow_schema = ArrowSchema::new(vec![Field::new("z", ArrowDataType::Int64, false)]);
    let batch = ArrowRecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![9_i64])) as _],
    )
    .unwrap();
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, Arc::new(arrow_schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let url = Url::from_file_path(&path).unwrap();
    let sm = try_build_fsr_footer_schema_sm(url.to_string()).expect("build SM");

    let ex = DataFusionExecutor::try_new().expect("executor");
    let out = ex
        .drive_coroutine_sm(sm, DriveOpts::default())
        .await
        .expect("drive");

    assert_eq!(out, FsrFooterSchemaOutcome { column_count: 1 });
}

//! Declarative Full Snapshot Read (FSR) scaffold: multi-phase [`CoroutineSM`] bodies composed with
//! the kernel [`Phase`](crate::plans::state_machines::framework::coroutine::phase::Phase) API.
//!
//! These builders are intentionally small — they mirror real FSR decomposition (checkpoint strip,
//! footer/schema probes, scan phases) without coupling to table directories yet. Engines such as
//! the DataFusion executor drive them via
//! [`StateMachine`](crate::plans::state_machines::framework::state_machine::StateMachine).

use crate::engine::arrow_data::ArrowEngineData;
use crate::expressions::Scalar;
use crate::plans::errors::{DeltaError, DeltaErrorCode, KernelErrAsDelta};
use crate::plans::ir::DeclarativePlanNode;
use crate::plans::kdf::{ConsumerKdf, KdfControl, KdfOutput};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::{PhaseOperation, SchemaQueryNode};
use crate::schema::SchemaRef;
use crate::{delta_error, DeltaResult, EngineData};

/// Row-count consumer used by FSR demo phases (checkpoint manifest strip → parallel discovery).
#[derive(Debug, Clone, Default)]
pub struct RowCounter(pub usize);

crate::impl_kdf!(RowCounter, "consumer.fsr.row_counter");

impl ConsumerKdf for RowCounter {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| crate::Error::generic("RowCounter: expected ArrowEngineData"))?;
        self.0 += arrow.record_batch().num_rows();
        Ok(KdfControl::Continue)
    }
}

impl KdfOutput for RowCounter {
    type Output = usize;

    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError> {
        Ok(parts.into_iter().map(|p| p.0).sum())
    }
}

/// Output of [`try_build_fsr_strip_then_fanout_sm`] — per-phase accumulated row counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsrStripThenFanoutOutcome {
    pub strip_rows: usize,
    pub fanout_rows: usize,
}

/// Two-phase FSR-shaped demo: consume literals as proxy "checkpoint strip" then "file discovery".
pub fn try_build_fsr_strip_then_fanout_sm(
    strip_schema: SchemaRef,
    strip_rows: Vec<Vec<Scalar>>,
    fanout_schema: SchemaRef,
    fanout_rows: Vec<Vec<Scalar>>,
) -> Result<CoroutineSM<FsrStripThenFanoutOutcome>, DeltaError> {
    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);

        let (strip_plan, strip_extractor) = DeclarativePlanNode::values(strip_schema, strip_rows)
            .map_err(|e| e.into_delta_default())?
            .consume(RowCounter::default());
        let strip_state = phase
            .execute(
                PhaseOperation::Plans(vec![strip_plan]),
                "fsr_strip_checkpoint_manifest",
            )
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::strip::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        let strip_rows = strip_extractor.extract(&strip_state).map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::strip::extract",
                detail = e.display_with_source_chain(),
                source = e,
            )
        })?;

        let (fan_plan, fan_extractor) = DeclarativePlanNode::values(fanout_schema, fanout_rows)
            .map_err(|e| e.into_delta_default())?
            .consume(RowCounter::default());
        let fan_state = phase
            .execute(
                PhaseOperation::Plans(vec![fan_plan]),
                "fsr_parallel_file_discovery",
            )
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::fanout::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        let fanout_rows = fan_extractor.extract(&fan_state).map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::fanout::extract",
                detail = e.display_with_source_chain(),
                source = e,
            )
        })?;

        Ok(FsrStripThenFanoutOutcome {
            strip_rows,
            fanout_rows,
        })
    })
}

/// Output of [`try_build_fsr_footer_schema_sm`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsrFooterSchemaOutcome {
    pub column_count: usize,
}

/// Single-phase footer schema probe (`PhaseOperation::SchemaQuery`).
pub fn try_build_fsr_footer_schema_sm(
    parquet_file_url: String,
) -> Result<CoroutineSM<FsrFooterSchemaOutcome>, DeltaError> {
    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        let state = phase
            .execute(
                PhaseOperation::SchemaQuery(SchemaQueryNode::new(parquet_file_url)),
                "fsr_read_footer_schema",
            )
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "fsr::footer_schema::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        // A successful SchemaQuery (no `EngineError`) MUST submit a schema; an
        // empty slot here means the executor finished the phase without
        // populating it, which is an executor contract bug -- surface it
        // rather than silently reporting zero columns.
        let schema = state.take_schema().ok_or_else(|| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "fsr::footer_schema::take_schema",
                detail = "executor reported PhaseOperation::SchemaQuery success but did not \
                          submit a schema",
            )
        })?;
        let column_count = schema.fields().len();
        Ok(FsrFooterSchemaOutcome { column_count })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plans::state_machines::framework::state_machine::StateMachine;
    use crate::schema::{DataType, StructField, StructType};

    #[test]
    fn demo_sm_starts_at_strip_phase() {
        let schema =
            Arc::new(StructType::try_new([StructField::not_null("x", DataType::LONG)]).unwrap());
        let sm = try_build_fsr_strip_then_fanout_sm(
            Arc::clone(&schema),
            vec![vec![Scalar::Long(1)]],
            schema,
            vec![vec![Scalar::Long(2)], vec![Scalar::Long(3)]],
        )
        .expect("build");
        assert_eq!(sm.phase_name(), "fsr_strip_checkpoint_manifest");
    }
}

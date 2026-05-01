//! Commit-action envelope collection for declarative DF replay (Phase 3.5).
//!
//! Rows are expected to carry a JSON object per Delta log action in column `action_json` (nullable
//! string). The consumer concatenates payloads in encounter order.

use std::any::Any;

use crate::arrow::array::{Array, AsArray};
use crate::engine::arrow_data::ArrowEngineData;
use crate::expressions::Scalar;
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::{DeclarativePlanNode, Extractor, Plan};
use crate::plans::kdf::{ConsumerKdf, Kdf, KdfControl, KdfOutput};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::Phase;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{delta_error, DeltaResult, EngineData};

/// Accumulates UTF-8 JSON lines (one Delta action JSON object per row) from column `action_json`.
#[derive(Clone, Debug, Default)]
pub struct CommitEnvelopeCollector {
    lines: Vec<String>,
}

impl Kdf for CommitEnvelopeCollector {
    fn kdf_id(&self) -> &'static str {
        "consumer.df.commit_action_envelope"
    }

    fn finish(self: Box<Self>) -> Box<dyn Any + Send> {
        Box::new(*self)
    }
}

impl ConsumerKdf for CommitEnvelopeCollector {
    fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<KdfControl> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| {
                crate::Error::generic("CommitEnvelopeCollector: expected ArrowEngineData")
            })?;
        let rb = arrow.record_batch();
        let col = rb.column(0);
        let strings = col.as_string::<i32>();
        for row in 0..rb.num_rows() {
            if strings.is_null(row) {
                continue;
            }
            self.lines.push(strings.value(row).to_string());
        }
        Ok(KdfControl::Continue)
    }
}

impl KdfOutput for CommitEnvelopeCollector {
    type Output = Vec<String>;

    fn into_output(parts: Vec<Self>) -> Result<Self::Output, DeltaError> {
        Ok(parts.into_iter().flat_map(|p| p.lines).collect())
    }
}

/// Schema: one nullable string column `action_json`.
pub fn commit_action_json_schema() -> SchemaRef {
    std::sync::Arc::new(StructType::new_unchecked([StructField::nullable(
        "action_json",
        DataType::STRING,
    )]))
}

/// Build a literal plan paired with a typed [`Extractor`] that drains the literal through a
/// [`CommitEnvelopeCollector`].
pub fn commit_action_envelopes_literal(
    json_lines: Vec<String>,
) -> DeltaResult<(Plan, Extractor<Vec<String>>)> {
    let schema = commit_action_json_schema();
    let rows: Vec<Vec<Scalar>> = json_lines
        .into_iter()
        .map(|s| vec![Scalar::String(s)])
        .collect();
    let node = DeclarativePlanNode::literal(schema, rows)?;
    Ok(node.consume(CommitEnvelopeCollector::default()))
}

/// SM: single phase — collect commit JSON envelopes via [`CommitEnvelopeCollector`].
pub fn commit_action_emit_sm(
    plan: Plan,
    extractor: Extractor<Vec<String>>,
) -> Result<CoroutineSM<Vec<String>>, DeltaError> {
    CoroutineSM::new(|mut co| async move {
        let mut phase = Phase(&mut co);
        let state = phase
            .execute(PhaseOperation::Plans(vec![plan]), "commit_action_emit")
            .await
            .map_err(|e| {
                delta_error!(
                    DeltaErrorCode::DeltaCommandInvariantViolation,
                    operation = "commit_action_emit_sm::execute",
                    detail = e.display_with_source_chain(),
                    source = e,
                )
            })?;
        extractor.extract(&state).map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "commit_action_emit_sm::extract",
                detail = e.display_with_source_chain(),
                source = e,
            )
        })
    })
}

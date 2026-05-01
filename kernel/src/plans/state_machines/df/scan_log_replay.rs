//! Scan log replay via declarative plans — Phase 4.x (`CoroutineSM` path).
//!
//! The canonical entrypoint is [`scan_log_replay_sm`], which returns a
//! [`CoroutineSM`](crate::plans::state_machines::framework::coroutine::driver::CoroutineSM)
//! that runs the JSON-commit dedup plan plus any checkpoint feeders in a single
//! [`PhaseOperation::Plans`] call.

use std::sync::Arc;

use crate::actions::{get_commit_schema, ADD_NAME, REMOVE_NAME};
use crate::expressions::{column_expr, column_expr_ref, Expression, Predicate, Scalar};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::plans::ir::nodes::{
    JoinHint, JoinNode, JoinType, RelationHandle, WindowFunction, WindowNode,
};
use crate::plans::ir::{DeclarativePlanNode, Plan};
use crate::plans::state_machines::framework::coroutine::driver::CoroutineSM;
use crate::plans::state_machines::framework::coroutine::phase::{Phase, PhaseCo};
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::scan::scan_row_schema;
use crate::schema::{DataType, SchemaRef, StructField, StructType};
use crate::{bail_delta, delta_error, FileMeta, Snapshot};

/// Synthetic top-level column carrying `coalesce(add.path, remove.path)` so DataFusion-backed
/// executors can hash-partition `row_number()` and anti-join without non-column expressions in
/// partition / join keys (see `delta-kernel-datafusion-engine` window + join compilers).
const DEDUP_KEY_COL: &str = "__kernel_path_key";

const PHASE_NAME: &str = "scan.log_replay_anti_join";
const PHASE1_RELATION_NAME: &str = "scan_log_replay.phase1Output";
const CHECKPOINT_RELATION_NAME: &str = "scan_log_replay.checkpointActions";
const ANTI_JOIN_RELATION_NAME: &str = "scan_log_replay.antiJoinResult";

struct ScanPlans {
    results: Plan,
    feeders: Vec<Plan>,
}

/// Declarative scan log replay as a [`CoroutineSM`] (preferred API).
///
/// See module-level docs for plan shape, checkpoint assumptions, and scope.
pub fn scan_log_replay_sm(snapshot: Arc<Snapshot>) -> Result<CoroutineSM<()>, DeltaError> {
    let plans = build_plans(&snapshot)?;
    CoroutineSM::new(move |co| {
        let plans = plans;
        async move { run_phase(co, plans).await }
    })
}

fn add_remove_read_schema() -> Result<SchemaRef, DeltaError> {
    get_commit_schema()
        .project(&[ADD_NAME, REMOVE_NAME])
        .map_err(|e| {
            crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "scan_log_replay::add_remove_read_schema",
                detail = format!("project commit schema to [add, remove]: {e}"),
            )
        })
}

fn read_schema_with_dedup_key(read_schema: &SchemaRef) -> Result<SchemaRef, DeltaError> {
    let mut fields: Vec<_> = read_schema.fields().cloned().collect();
    fields.push(StructField::nullable(DEDUP_KEY_COL, DataType::STRING));
    StructType::try_new(fields).map(Arc::new).map_err(|e| {
        crate::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "scan_log_replay::read_schema_with_dedup_key",
            detail = format!("extend commit read schema with dedup key column: {e}"),
        )
    })
}

/// `[add, remove]` columns unchanged plus [`DEDUP_KEY_COL`] = `coalesce(add.path, remove.path)`.
fn project_with_coalesce_path_key(
    read_schema: &SchemaRef,
) -> Result<(Vec<Arc<Expression>>, SchemaRef), DeltaError> {
    let ext = read_schema_with_dedup_key(read_schema)?;
    let mut exprs: Vec<_> = read_schema
        .fields()
        .map(|f| Arc::new(Expression::column([f.name().as_str()])))
        .collect();
    exprs.push(Arc::new(
        Expression::coalesce([
            Expression::column(["add", "path"]),
            Expression::column(["remove", "path"]),
        ])
        .into(),
    ));
    Ok((exprs, ext))
}

fn scan_row_projection() -> Vec<Arc<Expression>> {
    vec![
        column_expr_ref!("add.path"),
        column_expr_ref!("add.size"),
        column_expr_ref!("add.modificationTime"),
        column_expr_ref!("add.stats"),
        column_expr_ref!("add.deletionVector"),
        Arc::new(Expression::struct_from([
            column_expr_ref!("add.partitionValues"),
            column_expr_ref!("add.baseRowId"),
            column_expr_ref!("add.defaultRowCommitVersion"),
            column_expr_ref!("add.tags"),
            column_expr_ref!("add.clusteringProvider"),
        ])),
    ]
}

fn row_number_window_node() -> WindowNode {
    WindowNode {
        functions: vec![WindowFunction {
            function_name: "row_number".into(),
            args: vec![],
            output_col: "__kernel_rn".into(),
        }],
        partition_by: vec![Arc::new(Expression::column([DEDUP_KEY_COL]))],
        order_by: vec![],
    }
}

fn rn_equals_one() -> Arc<Expression> {
    Arc::new(
        column_expr!("__kernel_rn")
            .eq(Expression::literal(Scalar::Long(1)))
            .into(),
    )
}

fn commit_identity_projection(read_schema: &SchemaRef) -> Vec<Arc<Expression>> {
    read_schema
        .fields()
        .map(|f| Arc::new(Expression::column([f.name().as_str()])))
        .collect()
}

fn add_path_is_not_null() -> Arc<Expression> {
    Arc::new(column_expr!("add.path").is_not_null().into())
}

/// Commit / checkpoint scans can include non-file actions (for example `txn`). Drop those rows
/// before dedup so windowing and downstream projection only see `add` / `remove` file actions.
fn file_action_row_predicate() -> Arc<Expression> {
    Arc::new(
        Predicate::or(
            column_expr!("add.path").is_not_null(),
            column_expr!("remove.path").is_not_null(),
        )
        .into(),
    )
}

fn dedup_phase1_plan(
    scan: DeclarativePlanNode,
    read_schema: SchemaRef,
) -> Result<DeclarativePlanNode, DeltaError> {
    let scan = scan.filter(file_action_row_predicate());
    let (proj_exprs, ext_schema) = project_with_coalesce_path_key(&read_schema)?;
    let with_key = scan.project(proj_exprs, ext_schema.clone());
    let w = row_number_window_node();
    Ok(with_key
        .window(w.functions, w.partition_by, w.order_by)
        .filter(rn_equals_one())
        .project(commit_identity_projection(&ext_schema), ext_schema))
}

fn build_plans(snapshot: &Snapshot) -> Result<ScanPlans, DeltaError> {
    let log_segment = snapshot.log_segment();
    let commit_files: Vec<FileMeta> = log_segment.find_commit_cover();

    let checkpoint_parts = &log_segment.listed.checkpoint_parts;
    if checkpoint_parts.len() > 1 {
        bail_delta!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "scan_log_replay_sm::build_plans",
            detail = format!(
                "multi-part / V2 checkpoints are not yet supported (found {} checkpoint parts); \
                 see module docs for scope",
                checkpoint_parts.len(),
            ),
        );
    }
    let checkpoint_file: Option<FileMeta> =
        checkpoint_parts.first().map(|part| part.location.clone());

    let read_schema = add_remove_read_schema()?;
    let ext_schema = read_schema_with_dedup_key(&read_schema)?;

    let Some(checkpoint_file) = checkpoint_file else {
        let results = dedup_phase1_plan(
            DeclarativePlanNode::scan_json(commit_files, read_schema.clone()),
            read_schema.clone(),
        )?
        .filter(add_path_is_not_null())
        .project(scan_row_projection(), scan_row_schema())
        .results();
        return Ok(ScanPlans {
            results,
            feeders: Vec::new(),
        });
    };

    let phase1_handle = RelationHandle::fresh(PHASE1_RELATION_NAME, ext_schema.clone());
    let phase1 = dedup_phase1_plan(
        DeclarativePlanNode::scan_json(commit_files, read_schema.clone()),
        read_schema.clone(),
    )?
    .into_relation(phase1_handle.clone());

    let checkpoint_handle = RelationHandle::fresh(CHECKPOINT_RELATION_NAME, ext_schema.clone());
    let (ck_proj, ck_ext) = project_with_coalesce_path_key(&read_schema)?;
    debug_assert!(
        ck_ext.as_ref() == ext_schema.as_ref(),
        "checkpoint and commit dedup-key schemas must match"
    );
    let checkpoint = DeclarativePlanNode::scan_parquet(vec![checkpoint_file], read_schema.clone())
        .filter(add_path_is_not_null())
        .filter(file_action_row_predicate())
        .project(ck_proj, ext_schema.clone())
        .into_relation(checkpoint_handle.clone());

    let anti_join_handle = RelationHandle::fresh(ANTI_JOIN_RELATION_NAME, ext_schema.clone());
    let anti_join = DeclarativePlanNode::join(
        JoinNode {
            build_keys: vec![Arc::new(Expression::column([DEDUP_KEY_COL]))],
            probe_keys: vec![Arc::new(Expression::column([DEDUP_KEY_COL]))],
            join_type: JoinType::LeftAnti,
            hint: JoinHint::Hash,
        },
        DeclarativePlanNode::relation(phase1_handle.clone()),
        DeclarativePlanNode::relation(checkpoint_handle),
    )
    .into_relation(anti_join_handle.clone());

    let unioned = DeclarativePlanNode::union_unordered(vec![
        DeclarativePlanNode::relation(phase1_handle),
        DeclarativePlanNode::relation(anti_join_handle),
    ])
    .map_err(|e| {
        crate::delta_error!(
            DeltaErrorCode::DeltaCommandInvariantViolation,
            operation = "scan_log_replay_sm::build_plans",
            detail = format!("union construction: {e}"),
        )
    })?;
    let results = unioned
        .filter(add_path_is_not_null())
        .project(scan_row_projection(), scan_row_schema())
        .results();

    Ok(ScanPlans {
        results,
        feeders: vec![phase1, checkpoint, anti_join],
    })
}

async fn run_phase(mut co: PhaseCo, plans: ScanPlans) -> Result<(), DeltaError> {
    let ScanPlans { results, feeders } = plans;
    // Single-phase: feeders + results execute together as one PhaseOperation::Plans.
    // The DataFusion executor wires the relation handles, so feeders materialize before
    // results consumes them.
    let mut all_plans = feeders;
    all_plans.push(results);

    let mut phase = Phase(&mut co);
    let _state = phase
        .execute(PhaseOperation::Plans(all_plans), PHASE_NAME)
        .await
        .map_err(|e| {
            delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "scan_log_replay_sm::run_phase",
                detail = e.display_with_source_chain(),
                source = e,
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::state_machines::framework::state_machine::StateMachine;
    use crate::utils::test_utils::load_test_table;

    #[test]
    fn scan_log_replay_sm_first_phase_yields_plans() {
        let (_engine, snapshot, _tmp) = load_test_table("app-txn-no-checkpoint").unwrap();
        let mut sm = scan_log_replay_sm(snapshot).unwrap();
        match sm.get_operation().unwrap() {
            PhaseOperation::Plans(plans) => assert!(
                !plans.is_empty(),
                "no-checkpoint path should yield at least the results plan"
            ),
            other => panic!("expected Plans, got {other:?}"),
        }
    }
}

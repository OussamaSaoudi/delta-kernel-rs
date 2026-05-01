//! Smoke tests and TODOs for wiring the shared acceptance workload reader into the DataFusion
//! executor.
//!
//! Full end-to-end execution remains in the `acceptance` crate
//! (`acceptance/tests/acceptance_workloads_reader.rs`) because it couples table materialization,
//! kernel `Snapshot`/`Scan`, and result validation.
//!
//! ## TODO (Phase 1.9+)
//! - Add a `DataFusion`-backed code path where workload specs describe declarative IR / Results
//!   sinks, and delegate batch comparison to the same validators as DAT.
//! - Or: expose a thin `execute_declarative_plan_for_workload(plan, …)` helper here and call it
//!   from acceptance behind a feature flag once DF parity covers the workload subset.

use serde::Deserialize;

/// Minimal camelCase JSON shape compatible with workload read specs (`benchmarks` crate models).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MinimalWorkloadReadSpec {
    #[serde(rename = "type")]
    spec_type: String,
    predicate: Option<String>,
    #[serde(default)]
    columns: Option<Vec<String>>,
}

#[test]
fn acceptance_read_spec_json_round_trips_minimal_shape() {
    let json = r#"{
        "type": "read",
        "predicate": "id > 10",
        "columns": ["id", "name"],
        "expected": { "rowCount": 42 }
    }"#;
    let parsed: MinimalWorkloadReadSpec = serde_json::from_str(json).expect("parse workload JSON");
    assert_eq!(parsed.spec_type, "read");
    assert_eq!(parsed.predicate.as_deref(), Some("id > 10"));
    assert_eq!(
        parsed.columns.as_ref().map(|c| c.as_slice()),
        Some(&["id".to_string(), "name".to_string()][..])
    );
}

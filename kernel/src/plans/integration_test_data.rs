//! Creates test proto data files for cross-language integration testing.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use prost::Message;

use crate::expressions::column_expr;
use crate::plans::*;
use crate::proto_generated as proto;
use crate::schema::{DataType, StructField, StructType};

/// Generate test proto data files for Java integration tests.
pub fn generate_test_files(output_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(output_dir)?;

    // Test 1: LogReplayPhase (Commit)
    let commit_bytes = create_commit_phase_bytes();
    fs::write(output_dir.join("log_replay_commit.bin"), &commit_bytes)?;

    // Test 2: DeclarativePlanNode tree
    let plan_bytes = create_declarative_plan_bytes();
    fs::write(output_dir.join("declarative_plan.bin"), &plan_bytes)?;

    // Test 3: Complete phase
    let complete_bytes = create_complete_phase_bytes();
    fs::write(output_dir.join("log_replay_complete.bin"), &complete_bytes)?;

    Ok(())
}

fn create_commit_phase_bytes() -> Vec<u8> {
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("path", DataType::STRING, false),
        StructField::new("size", DataType::LONG, true),
    ]));

    let commit_plan = CommitPhasePlan {
        scan: ScanNode {
            file_type: FileType::Json,
            files: vec![],
            schema: schema.clone(),
        },
        data_skipping: None,
        dedup_filter: FilterByKDF::add_remove_dedup(),
        project: SelectNode {
            columns: vec![
                Arc::new(column_expr!("path")),
                Arc::new(column_expr!("size")),
            ],
            output_schema: schema,
        },
    };

    let phase = LogReplayPhase::Commit(commit_plan);
    let proto_phase: proto::LogReplayPhase = (&phase).into();
    proto_phase.encode_to_vec()
}

fn create_declarative_plan_bytes() -> Vec<u8> {
    let schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("json_col", DataType::STRING, true),
    ]));

    let parsed_schema = Arc::new(StructType::new_unchecked(vec![
        StructField::new("id", DataType::INTEGER, false),
        StructField::new("parsed", DataType::STRING, true),
    ]));

    // Build: Filter -> ParseJson -> Scan
    let scan = DeclarativePlanNode::Scan(ScanNode {
        file_type: FileType::Parquet,
        files: vec![],
        schema,
    });

    let parse_json = DeclarativePlanNode::ParseJson {
        child: Box::new(scan),
        node: ParseJsonNode {
            json_column: "json_col".to_string(),
            target_schema: parsed_schema,
            output_column: "parsed".to_string(),
        },
    };

    let filter = DeclarativePlanNode::FilterByKDF {
        child: Box::new(parse_json),
        node: FilterByKDF::add_remove_dedup(),
    };

    let proto_plan: proto::DeclarativePlanNode = (&filter).into();
    proto_plan.encode_to_vec()
}

fn create_complete_phase_bytes() -> Vec<u8> {
    let phase = LogReplayPhase::Complete;
    let proto_phase: proto::LogReplayPhase = (&phase).into();
    proto_phase.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn generate_integration_test_data() {
        // Write to a known location for Java tests to read
        let output_dir = env::current_dir()
            .unwrap()
            .join("target")
            .join("proto-test-data");

        generate_test_files(&output_dir).expect("Failed to generate test files");

        println!("Generated proto test data in: {}", output_dir.display());

        // Verify files were created
        assert!(output_dir.join("log_replay_commit.bin").exists());
        assert!(output_dir.join("declarative_plan.bin").exists());
        assert!(output_dir.join("log_replay_complete.bin").exists());
    }
}

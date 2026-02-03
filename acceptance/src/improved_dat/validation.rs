//! Result validation for improved_dat test cases.

use std::path::Path;
use std::sync::Arc;

use delta_kernel::arrow::array::{Array, RecordBatch};
use delta_kernel::arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use delta_kernel::arrow::datatypes::DataType;
use delta_kernel::arrow::util::pretty::pretty_format_batches;
use delta_kernel::parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStreamBuilder,
};
use delta_kernel::DeltaResult;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use super::types::{ExpectedMetadata, ExpectedProtocol, ExpectedSummary};
use super::workload::SnapshotResult;

/// Validation error
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Row count mismatch: expected {expected}, got {actual}")]
    RowCountMismatch { expected: u64, actual: u64 },
    #[error("Column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("Data mismatch at column {column}: {message}")]
    DataMismatch { column: String, message: String },
    #[error("Protocol mismatch: {message}")]
    ProtocolMismatch { message: String },
    #[error("Metadata mismatch: {message}")]
    MetadataMismatch { message: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Kernel error: {0}")]
    Kernel(#[from] delta_kernel::Error),
}

/// Sort a record batch lexicographically by all sortable columns
pub fn sort_record_batch(batch: RecordBatch) -> DeltaResult<RecordBatch> {
    let mut sort_columns = vec![];
    for col in batch.columns() {
        match col.data_type() {
            DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _) => {
                // can't sort structs, lists, or maps
            }
            _ => sort_columns.push(SortColumn {
                values: col.clone(),
                options: None,
            }),
        }
    }

    if sort_columns.is_empty() {
        return Ok(batch);
    }

    let indices = lexsort_to_indices(&sort_columns, None)?;
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Normalize a column for comparison (handle timezone equivalence)
fn normalize_col(col: Arc<dyn Array>) -> Arc<dyn Array> {
    if let DataType::Timestamp(unit, Some(zone)) = col.data_type() {
        if **zone == *"+00:00" {
            let data_type = DataType::Timestamp(*unit, Some("UTC".into()));
            return delta_kernel::arrow::compute::cast(&col, &data_type)
                .expect("Could not cast to UTC");
        }
    }
    col
}

/// Strip field metadata from a data type (recursive for structs)
fn strip_metadata_from_datatype(dt: &DataType) -> DataType {
    use delta_kernel::arrow::datatypes::Field;

    match dt {
        DataType::Struct(fields) => {
            let new_fields: Vec<_> = fields
                .iter()
                .map(|f| {
                    let new_dt = strip_metadata_from_datatype(f.data_type());
                    Arc::new(Field::new(f.name(), new_dt, f.is_nullable()))
                })
                .collect();
            DataType::Struct(new_fields.into())
        }
        DataType::List(field) => {
            let new_dt = strip_metadata_from_datatype(field.data_type());
            DataType::List(Arc::new(Field::new(field.name(), new_dt, field.is_nullable())).into())
        }
        DataType::Map(field, sorted) => {
            let new_dt = strip_metadata_from_datatype(field.data_type());
            DataType::Map(
                Arc::new(Field::new(field.name(), new_dt, field.is_nullable())).into(),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

/// Cast an array to strip field metadata (for struct comparisons)
fn strip_metadata_from_array(arr: &Arc<dyn Array>) -> Arc<dyn Array> {
    let target_type = strip_metadata_from_datatype(arr.data_type());
    if &target_type == arr.data_type() {
        arr.clone()
    } else {
        delta_kernel::arrow::compute::cast(arr, &target_type)
            .expect("Failed to cast array to strip metadata")
    }
}

/// Compare two sets of columns for equality (ignoring field metadata)
pub fn columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    for (actual, expected) in actual.iter().zip(expected) {
        let actual = strip_metadata_from_array(&normalize_col(actual.clone()));
        let expected = strip_metadata_from_array(&normalize_col(expected.clone()));
        if &actual != &expected {
            return false;
        }
    }
    true
}

/// Assert that two sets of columns match
pub fn assert_columns_match(actual: &[Arc<dyn Array>], expected: &[Arc<dyn Array>]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Column count mismatch: actual={}, expected={}",
        actual.len(),
        expected.len()
    );
    for (i, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        let actual = normalize_col(actual.clone());
        let expected = normalize_col(expected.clone());
        assert_eq!(
            &actual, &expected,
            "Column {} data mismatch. Got {:?}, expected {:?}",
            i, actual, expected
        );
    }
}

/// Read expected data from parquet files in the given directory
pub async fn read_expected_data(expected_dir: &Path) -> DeltaResult<Option<RecordBatch>> {
    let expected_data_dir = expected_dir.join("expected_data");
    if !expected_data_dir.exists() {
        return Ok(None);
    }

    let store = Arc::new(LocalFileSystem::new_with_prefix(&expected_data_dir)?);
    let files: Vec<_> = store
        .list(None)
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    let mut batches = vec![];
    let mut schema = None;

    for meta in files {
        if let Some(ext) = meta.location.extension() {
            if ext == "parquet" {
                let reader = ParquetObjectReader::new(store.clone(), meta.location);
                let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
                if schema.is_none() {
                    schema = Some(builder.schema().clone());
                }
                let mut stream = builder.build()?;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
            }
        }
    }

    if let Some(schema) = schema {
        let all_data = concat_batches(&schema, &batches)?;
        Ok(Some(all_data))
    } else {
        Ok(None)
    }
}

/// Read and parse the summary.json file
pub fn read_expected_summary(expected_dir: &Path) -> Result<Option<ExpectedSummary>, ValidationError> {
    let summary_path = expected_dir.join("summary.json");
    if !summary_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(summary_path)?;
    let summary: ExpectedSummary =
        serde_json::from_reader(file).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(summary))
}

/// Read and parse the protocol.json file
pub fn read_expected_protocol(expected_dir: &Path) -> Result<Option<ExpectedProtocol>, ValidationError> {
    let protocol_path = expected_dir.join("protocol.json");
    if !protocol_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(protocol_path)?;
    let wrapper: super::types::ProtocolWrapper =
        serde_json::from_reader(file).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(wrapper.protocol))
}

/// Read and parse the metadata.json file
pub fn read_expected_metadata(expected_dir: &Path) -> Result<Option<ExpectedMetadata>, ValidationError> {
    let metadata_path = expected_dir.join("metadata.json");
    if !metadata_path.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(metadata_path)?;
    let wrapper: super::types::MetadataWrapper =
        serde_json::from_reader(file).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Some(wrapper.meta_data))
}

/// Validate read results against expected data
pub async fn validate_read_result(
    actual: RecordBatch,
    expected_dir: &Path,
) -> Result<(), ValidationError> {
    // Get actual row count before we potentially move the batch
    let actual_row_count = actual.num_rows() as u64;

    // Read expected data
    let expected = read_expected_data(expected_dir).await?;

    if let Some(expected) = expected {
        // Sort both for comparison
        let actual = sort_record_batch(actual)?;
        let expected = sort_record_batch(expected)?;

        // Check row count
        if actual.num_rows() != expected.num_rows() {
            eprintln!("\n=== DATA MISMATCH ===");
            eprintln!("Expected {} rows, got {} rows", expected.num_rows(), actual.num_rows());
            eprintln!("\n--- Expected Data ---");
            eprintln!("{}", pretty_format_batches(&[expected.clone()]).map(|d| d.to_string()).unwrap_or_else(|_| "Failed to format".to_string()));
            eprintln!("\n--- Actual Data ---");
            eprintln!("{}", pretty_format_batches(&[actual.clone()]).map(|d| d.to_string()).unwrap_or_else(|_| "Failed to format".to_string()));
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::RowCountMismatch {
                expected: expected.num_rows() as u64,
                actual: actual.num_rows() as u64,
            });
        }

        // Check columns match
        if !columns_match(actual.columns(), expected.columns()) {
            eprintln!("\n=== DATA MISMATCH ===");
            eprintln!("Column data does not match");
            eprintln!("\n--- Expected Schema ---");
            eprintln!("{:#?}", expected.schema());
            eprintln!("\n--- Actual Schema ---");
            eprintln!("{:#?}", actual.schema());
            eprintln!("\n--- Expected Data ---");
            eprintln!("{}", pretty_format_batches(&[expected.clone()]).map(|d| d.to_string()).unwrap_or_else(|_| "Failed to format".to_string()));
            eprintln!("\n--- Actual Data ---");
            eprintln!("{}", pretty_format_batches(&[actual.clone()]).map(|d| d.to_string()).unwrap_or_else(|_| "Failed to format".to_string()));
            eprintln!("=== END MISMATCH ===\n");
            return Err(ValidationError::DataMismatch {
                column: "unknown".to_string(),
                message: "Data content does not match".to_string(),
            });
        }
    }

    // Also validate against summary.json if present
    if let Some(summary) = read_expected_summary(expected_dir)? {
        if actual_row_count != summary.expected_row_count {
            return Err(ValidationError::RowCountMismatch {
                expected: summary.expected_row_count,
                actual: actual_row_count,
            });
        }
    }

    Ok(())
}

/// Validate snapshot metadata against expected protocol and metadata
pub fn validate_snapshot_metadata(
    result: &SnapshotResult,
    expected_dir: &Path,
) -> Result<(), ValidationError> {
    // Validate protocol
    if let Some(expected_protocol) = read_expected_protocol(expected_dir)? {
        if result.min_reader_version != expected_protocol.min_reader_version {
            return Err(ValidationError::ProtocolMismatch {
                message: format!(
                    "minReaderVersion mismatch: expected {}, got {}",
                    expected_protocol.min_reader_version, result.min_reader_version
                ),
            });
        }
        if result.min_writer_version != expected_protocol.min_writer_version {
            return Err(ValidationError::ProtocolMismatch {
                message: format!(
                    "minWriterVersion mismatch: expected {}, got {}",
                    expected_protocol.min_writer_version, result.min_writer_version
                ),
            });
        }

        // Check reader features if present
        if let Some(expected_features) = &expected_protocol.reader_features {
            let actual_set: std::collections::HashSet<_> = result.reader_features.iter().collect();
            let expected_set: std::collections::HashSet<_> = expected_features.iter().collect();
            if actual_set != expected_set {
                return Err(ValidationError::ProtocolMismatch {
                    message: format!(
                        "readerFeatures mismatch: expected {:?}, got {:?}",
                        expected_features, result.reader_features
                    ),
                });
            }
        }

        // Check writer features if present
        if let Some(expected_features) = &expected_protocol.writer_features {
            let actual_set: std::collections::HashSet<_> = result.writer_features.iter().collect();
            let expected_set: std::collections::HashSet<_> = expected_features.iter().collect();
            if actual_set != expected_set {
                return Err(ValidationError::ProtocolMismatch {
                    message: format!(
                        "writerFeatures mismatch: expected {:?}, got {:?}",
                        expected_features, result.writer_features
                    ),
                });
            }
        }
    }

    // Validate metadata
    if let Some(expected_metadata) = read_expected_metadata(expected_dir)? {
        // Check table ID
        if result.table_id != expected_metadata.id {
            return Err(ValidationError::MetadataMismatch {
                message: format!(
                    "Table ID mismatch: expected {}, got {}",
                    expected_metadata.id, result.table_id
                ),
            });
        }

        // Check partition columns
        if result.partition_columns != expected_metadata.partition_columns {
            return Err(ValidationError::MetadataMismatch {
                message: format!(
                    "Partition columns mismatch: expected {:?}, got {:?}",
                    expected_metadata.partition_columns, result.partition_columns
                ),
            });
        }

        // Check configuration
        for (key, expected_value) in &expected_metadata.configuration {
            match result.configuration.get(key) {
                Some(actual_value) if actual_value == expected_value => {}
                Some(actual_value) => {
                    return Err(ValidationError::MetadataMismatch {
                        message: format!(
                            "Configuration '{}' mismatch: expected '{}', got '{}'",
                            key, expected_value, actual_value
                        ),
                    });
                }
                None => {
                    return Err(ValidationError::MetadataMismatch {
                        message: format!(
                            "Configuration '{}' missing: expected '{}'",
                            key, expected_value
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Validate domain metadata against expected values
pub fn validate_domain_metadata(
    result: &super::workload::DomainMetadataResult,
    expected: &super::types::ExpectedDomainMetadata,
) -> Result<(), ValidationError> {
    // Check if domain matches
    if result.domain != expected.domain {
        return Err(ValidationError::MetadataMismatch {
            message: format!(
                "Domain name mismatch: expected '{}', got '{}'",
                expected.domain, result.domain
            ),
        });
    }

    // Check removed status
    if expected.removed {
        // Domain should be removed (configuration should be None)
        if result.configuration.is_some() {
            return Err(ValidationError::MetadataMismatch {
                message: format!(
                    "Domain '{}' should be removed but has configuration: {:?}",
                    expected.domain, result.configuration
                ),
            });
        }
    } else {
        // Domain should exist with expected configuration
        match &result.configuration {
            Some(actual_config) => {
                if actual_config != &expected.configuration {
                    return Err(ValidationError::MetadataMismatch {
                        message: format!(
                            "Domain '{}' configuration mismatch: expected '{}', got '{}'",
                            expected.domain, expected.configuration, actual_config
                        ),
                    });
                }
            }
            None => {
                return Err(ValidationError::MetadataMismatch {
                    message: format!(
                        "Domain '{}' not found but expected configuration: '{}'",
                        expected.domain, expected.configuration
                    ),
                });
            }
        }
    }

    Ok(())
}

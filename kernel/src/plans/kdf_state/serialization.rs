//! Shared serialization helpers for KDF states.

use std::collections::HashSet;

use crate::log_replay::FileActionKey;
use crate::{DeltaResult, Error};

/// Serialize a HashSet of FileActionKeys to Arrow IPC bytes.
///
/// Converts the set to two parallel StringArray columns (path, dv_unique_id)
/// and writes as an Arrow IPC stream.
pub(crate) fn serialize_file_action_keys(keys: &HashSet<FileActionKey>) -> DeltaResult<Vec<u8>> {
    use crate::arrow::array::{RecordBatch, StringArray};
    use crate::arrow::datatypes::{DataType, Field, Schema};
    use crate::arrow::ipc::writer::StreamWriter;
    use std::sync::Arc;

    // Collect paths and dv_unique_ids from the HashSet
    let paths: Vec<&str> = keys.iter().map(|k| k.path.as_str()).collect();
    let dv_ids: Vec<Option<&str>> = keys.iter().map(|k| k.dv_unique_id.as_deref()).collect();

    // Create Arrow arrays
    let path_array = StringArray::from(paths);
    let dv_id_array = StringArray::from(dv_ids);

    // Create schema and RecordBatch
    let schema = Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("dv_unique_id", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(path_array), Arc::new(dv_id_array)],
    )
    .map_err(|e| Error::generic(format!("Failed to create RecordBatch: {}", e)))?;

    // Write to IPC stream
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| Error::generic(format!("Failed to create IPC writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| Error::generic(format!("Failed to write batch: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::generic(format!("Failed to finish IPC stream: {}", e)))?;
    }

    Ok(buffer)
}

/// Deserialize a HashSet of FileActionKeys from Arrow IPC bytes.
///
/// Reads an Arrow IPC stream and reconstructs the HashSet from the
/// path and dv_unique_id columns.
pub(crate) fn deserialize_file_action_keys(bytes: &[u8]) -> DeltaResult<HashSet<FileActionKey>> {
    use crate::arrow::array::{Array, StringArray};
    use crate::arrow::ipc::reader::StreamReader;
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| Error::generic(format!("Failed to create IPC reader: {}", e)))?;

    let batch = reader
        .next()
        .ok_or_else(|| Error::generic("No batch in IPC stream"))?
        .map_err(|e| Error::generic(format!("Failed to read batch: {}", e)))?;

    // Extract columns
    let path_col = batch
        .column_by_name("path")
        .ok_or_else(|| Error::generic("Missing 'path' column in serialized state"))?;
    let path_array = path_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::generic("'path' column is not a StringArray"))?;

    let dv_id_col = batch
        .column_by_name("dv_unique_id")
        .ok_or_else(|| Error::generic("Missing 'dv_unique_id' column in serialized state"))?;
    let dv_id_array = dv_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::generic("'dv_unique_id' column is not a StringArray"))?;

    // Reconstruct HashSet
    let mut keys = HashSet::new();
    for i in 0..batch.num_rows() {
        let path = path_array.value(i).to_string();
        let dv_unique_id = if dv_id_array.is_null(i) {
            None
        } else {
            Some(dv_id_array.value(i).to_string())
        };
        keys.insert(FileActionKey { path, dv_unique_id });
    }

    Ok(keys)
}

/// Helper function for getting selection value or defaulting to true.
///
/// Used by Filter KDFs to handle selection vectors that may be shorter than the batch.
#[inline]
pub(crate) fn selection_value_or_true(selection: &crate::arrow::array::BooleanArray, row: usize) -> bool {
    if row < selection.len() {
        selection.value(row)
    } else {
        true
    }
}



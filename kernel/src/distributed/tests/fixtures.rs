//! Test fixtures and utilities for distributed log replay tests.

use std::path::PathBuf;
use std::sync::Arc;

use url::Url;

use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
use crate::engine::default::DefaultEngine;
use crate::log_segment::LogSegment;
use crate::snapshot::Snapshot;
use crate::DeltaResult;

/// Get a test table path.
pub fn get_test_table_path(table_name: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/data");
    path.push(table_name);
    path
}

/// Create engine and load a test table snapshot.
pub fn load_test_table(
    table_name: &str,
) -> DeltaResult<(
    Arc<DefaultEngine<TokioBackgroundExecutor>>,
    Arc<Snapshot>,
    Url,
)> {
    let path = std::fs::canonicalize(get_test_table_path(table_name))
        .map_err(|e| crate::Error::Generic(format!("Failed to canonicalize path: {}", e)))?;
    
    let url = Url::from_directory_path(path)
        .map_err(|_| crate::Error::Generic("Failed to create URL from path".to_string()))?;
    
    let store = Arc::new(object_store::local::LocalFileSystem::new());
    let engine = Arc::new(DefaultEngine::new(store));
    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
    
    Ok((engine, snapshot, url))
}

/// Get the log segment from a test table.
pub fn get_log_segment(snapshot: &Snapshot) -> Arc<LogSegment> {
    Arc::new(snapshot.log_segment().clone())
}

/// Compare two iterators of scan metadata for equality.
///
/// This is used to verify that distributed implementations produce the same
/// results as the ground truth implementation.
pub fn compare_scan_results<I1, I2>(
    mut iter1: I1,
    mut iter2: I2,
    context: &str,
) -> DeltaResult<()>
where
    I1: Iterator<Item = DeltaResult<crate::scan::ScanMetadata>>,
    I2: Iterator<Item = DeltaResult<crate::scan::ScanMetadata>>,
{
    let mut batch_count = 0;
    loop {
        let item1 = iter1.next().transpose()?;
        let item2 = iter2.next().transpose()?;

        match (item1, item2) {
            (None, None) => break,
            (Some(_), None) => {
                return Err(crate::Error::Generic(format!(
                    "{}: iter1 has more items than iter2 at batch {}",
                    context, batch_count
                )))
            }
            (None, Some(_)) => {
                return Err(crate::Error::Generic(format!(
                    "{}: iter2 has more items than iter1 at batch {}",
                    context, batch_count
                )))
            }
            (Some(meta1), Some(meta2)) => {
                // Compare selection vectors
                let sv1 = meta1.scan_files.selection_vector();
                let sv2 = meta2.scan_files.selection_vector();
                
                if sv1 != sv2 {
                    return Err(crate::Error::Generic(format!(
                        "{}: selection vectors differ at batch {}",
                        context, batch_count
                    )));
                }

                // Compare row counts
                let data1 = meta1.scan_files.data();
                let data2 = meta2.scan_files.data();
                
                if data1.len() != data2.len() {
                    return Err(crate::Error::Generic(format!(
                        "{}: row counts differ at batch {} ({} vs {})",
                        context, batch_count, data1.len(), data2.len()
                    )));
                }

                // Compare transforms count
                if meta1.scan_file_transforms.len() != meta2.scan_file_transforms.len() {
                    return Err(crate::Error::Generic(format!(
                        "{}: transform counts differ at batch {}",
                        context, batch_count
                    )));
                }

                batch_count += 1;
            }
        }
    }

    Ok(())
}

/// Extract file paths from scan metadata for comparison.
pub fn extract_file_paths(
    iter: impl Iterator<Item = DeltaResult<crate::scan::ScanMetadata>>,
) -> DeltaResult<Vec<String>> {
    let mut all_paths = Vec::new();

    for meta in iter {
        let meta = meta?;
        let paths = meta.visit_scan_files(vec![], |paths: &mut Vec<String>, path, _, _, _, _, _| {
            paths.push(path.to_string());
        })?;
        all_paths.extend(paths);
    }

    Ok(all_paths)
}


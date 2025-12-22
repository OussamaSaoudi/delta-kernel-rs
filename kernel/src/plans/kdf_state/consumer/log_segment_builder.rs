//! LogSegmentBuilder Consumer KDF - builds a LogSegment from file listing results.

use url::Url;

use crate::listed_log_files::ListedLogFiles;
use crate::log_segment::LogSegment;
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::{DeltaResult, EngineData, Error, FileMeta, Version};

/// State for LogSegmentBuilder consumer KDF - builds a LogSegment from file listing.
///
/// Uses RowVisitor pattern to eliminate downcasting and column extraction boilerplate.
/// Direct mutation (no interior mutability) - owned state is mutated and passed back.
#[derive(Debug, Clone)]
pub struct LogSegmentBuilderState {
    /// Log directory root URL
    log_root: Url,
    /// Optional end version to stop at
    end_version: Option<Version>,
    /// Checkpoint hint version from `_last_checkpoint` file
    checkpoint_hint_version: Option<Version>,
    /// Sorted commit files in the log segment (ascending)
    ascending_commit_files: Vec<ParsedLogPath>,
    /// Sorted compaction files in the log segment (ascending)
    ascending_compaction_files: Vec<ParsedLogPath>,
    /// Checkpoint files in the log segment
    checkpoint_parts: Vec<ParsedLogPath>,
    /// Latest CRC (checksum) file
    latest_crc_file: Option<ParsedLogPath>,
    /// Latest commit file (may not be in contiguous segment)
    latest_commit_file: Option<ParsedLogPath>,
    /// Stored error to surface during advance()
    error: Option<String>,
    /// Current group version for checkpoint grouping
    current_group_version: Option<Version>,
    /// New checkpoint parts being accumulated for current version
    new_checkpoint_parts: Vec<ParsedLogPath>,
}

impl LogSegmentBuilderState {
    /// Create new state for building a LogSegment.
    pub fn new(
        log_root: Url,
        end_version: Option<Version>,
        checkpoint_hint_version: Option<Version>,
    ) -> Self {
        Self {
            log_root,
            end_version,
            checkpoint_hint_version,
            ascending_commit_files: Vec::new(),
            ascending_compaction_files: Vec::new(),
            checkpoint_parts: Vec::new(),
            latest_crc_file: None,
            latest_commit_file: None,
            error: None,
            current_group_version: None,
            new_checkpoint_parts: Vec::new(),
        }
    }

    /// Apply consumer to a batch of file metadata using RowVisitor pattern.
    #[inline]
    pub fn apply(&mut self, batch: &dyn EngineData) -> DeltaResult<bool> {
        use crate::engine_data::{GetData, RowVisitor, TypedGetData as _};
        use crate::schema::{ColumnName, DataType};

        // If we already have an error, stop processing
        if self.error.is_some() {
            return Ok(false);
        }

        struct LogSegmentVisitor<'a> {
            state: &'a mut LogSegmentBuilderState,
            should_continue: bool,
        }

        impl crate::engine_data::RowVisitor for LogSegmentVisitor<'_> {
            fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
                const STRING: DataType = DataType::STRING;
                const LONG: DataType = DataType::LONG;
                crate::column_names_and_types![
                    STRING => crate::schema::column_name!("path"),
                    LONG => crate::schema::column_name!("size"),
                    LONG => crate::schema::column_name!("modificationTime"),
                ]
            }

            fn visit<'a>(&mut self, row_count: usize, getters: &[&'a dyn GetData<'a>]) -> DeltaResult<()> {
                for i in 0..row_count {
                    let Some(path_str) = getters[0].get_str(i, "path")? else {
                        continue; // Skip null paths
                    };
                    
                    let size = getters[1].get_long(i, "size")?.unwrap_or(0) as u64;
                    let mod_time = getters[2].get_long(i, "modificationTime")?.unwrap_or(0);

                    // Parse the path into a URL
                    let file_url = match self.state.log_root.join(path_str) {
                        Ok(url) => url,
                        Err(e) => {
                            self.state.error = Some(format!("Failed to parse file path '{}': {}", path_str, e));
                            self.should_continue = false;
                            return Ok(());
                        }
                    };

                    // Create FileMeta
                    let file_meta = FileMeta {
                        location: file_url,
                        last_modified: mod_time,
                        size,
                    };

                    // Try to parse as ParsedLogPath
                    let parsed_path = match ParsedLogPath::try_from(file_meta) {
                        Ok(Some(path)) => path,
                        Ok(None) => continue, // Not a valid log path, skip
                        Err(e) => {
                            self.state.error = Some(format!("Failed to parse log path '{}': {}", path_str, e));
                            self.should_continue = false;
                            return Ok(());
                        }
                    };

                    // Check if we should stop based on end_version
                    if let Some(end_version) = self.state.end_version {
                        if parsed_path.version > end_version {
                            // Flush any pending checkpoint group before stopping
                            if let Some(group_version) = self.state.current_group_version {
                                self.state.flush_checkpoint_group(group_version);
                            }
                            self.should_continue = false;
                            return Ok(());
                        }
                    }

                    // Process the file based on its type
                    self.state.process_file(parsed_path);
                }
                Ok(())
            }
        }

        let mut visitor = LogSegmentVisitor {
            state: self,
            should_continue: true,
        };

        visitor.visit_rows_of(batch)?;
        Ok(visitor.should_continue)
    }

    /// Process a single parsed log file.
    fn process_file(&mut self, file: ParsedLogPath) {
        // Check if version changed - need to flush checkpoint group
        if let Some(group_version) = self.current_group_version {
            if file.version != group_version {
                self.flush_checkpoint_group(group_version);
            }
        }
        self.current_group_version = Some(file.version);

        match &file.file_type {
            LogPathFileType::Commit | LogPathFileType::StagedCommit => {
                self.ascending_commit_files.push(file);
            }
            LogPathFileType::CompactedCommit { hi } => {
                // Only include if within end_version bounds
                if self.end_version.is_none_or(|end| *hi <= end) {
                    self.ascending_compaction_files.push(file);
                }
            }
            LogPathFileType::SinglePartCheckpoint
            | LogPathFileType::UuidCheckpoint
            | LogPathFileType::MultiPartCheckpoint { .. } => {
                self.new_checkpoint_parts.push(file);
            }
            LogPathFileType::Crc => {
                self.latest_crc_file = Some(file);
            }
            LogPathFileType::Unknown => {
                // Ignore unknown file types
            }
        }
    }

    /// Flush accumulated checkpoint parts for a version.
    fn flush_checkpoint_group(&mut self, version: Version) {
        if self.new_checkpoint_parts.is_empty() {
            return;
        }

        // Group and find complete checkpoint
        let new_parts = std::mem::take(&mut self.new_checkpoint_parts);
        if let Some(complete_checkpoint) = Self::find_complete_checkpoint(new_parts) {
            self.checkpoint_parts = complete_checkpoint;
            // Save latest commit at checkpoint version if exists
            self.latest_commit_file = self
                .ascending_commit_files
                .pop()
                .filter(|commit| commit.version == version);
            // Clear commits/compactions before checkpoint
            self.ascending_commit_files.clear();
            self.ascending_compaction_files.clear();
        }
    }

    /// Find a complete checkpoint from parts (static helper).
    fn find_complete_checkpoint(parts: Vec<ParsedLogPath>) -> Option<Vec<ParsedLogPath>> {
        use std::collections::HashMap;

        let mut checkpoints: HashMap<u32, Vec<ParsedLogPath>> = HashMap::new();

        for part_file in parts {
            match &part_file.file_type {
                LogPathFileType::SinglePartCheckpoint
                | LogPathFileType::UuidCheckpoint
                | LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts: 1,
                } => {
                    // Single-file checkpoints are equivalent, keep one
                    checkpoints.insert(1, vec![part_file]);
                }
                LogPathFileType::MultiPartCheckpoint {
                    part_num: 1,
                    num_parts,
                } => {
                    checkpoints.insert(*num_parts, vec![part_file]);
                }
                LogPathFileType::MultiPartCheckpoint { part_num, num_parts } => {
                    if let Some(part_files) = checkpoints.get_mut(num_parts) {
                        if *part_num as usize == 1 + part_files.len() {
                            part_files.push(part_file);
                        }
                    }
                }
                _ => {}
            }
        }

        // Find first complete checkpoint
        checkpoints
            .into_iter()
            .find(|(num_parts, part_files)| part_files.len() == *num_parts as usize)
            .map(|(_, parts)| parts)
    }

    /// Finalize the state and check for errors.
    pub fn finalize(&mut self) {
        // Flush final checkpoint group
        if let Some(group_version) = self.current_group_version {
            self.flush_checkpoint_group(group_version);
        }

        // Update latest_commit_file if we have commits after checkpoint
        if let Some(commit_file) = self.ascending_commit_files.last() {
            self.latest_commit_file = Some(commit_file.clone());
        }
    }

    /// Check if an error occurred during processing.
    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Take the error, if any.
    pub fn take_error(&mut self) -> Option<String> {
        self.error.take()
    }

    /// Convert the accumulated state into a LogSegment.
    pub fn into_log_segment(mut self) -> DeltaResult<LogSegment> {
        // Finalize first
        self.finalize();

        // Check for errors
        if let Some(error) = self.error.take() {
            return Err(Error::generic(error));
        }

        // Build ListedLogFiles
        let listed_files = ListedLogFiles::try_new(
            self.ascending_commit_files,
            self.ascending_compaction_files,
            self.checkpoint_parts,
            self.latest_crc_file,
            self.latest_commit_file,
        )?;

        // Build LogSegment
        LogSegment::try_new(listed_files, self.log_root, self.end_version)
    }

    // Test helper accessors
    #[cfg(test)]
    pub(crate) fn log_root(&self) -> &Url {
        &self.log_root
    }

    #[cfg(test)]
    pub(crate) fn end_version(&self) -> Option<Version> {
        self.end_version
    }

    #[cfg(test)]
    pub(crate) fn ascending_commit_files(&self) -> &[ParsedLogPath] {
        &self.ascending_commit_files
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_parts(&self) -> &[ParsedLogPath] {
        &self.checkpoint_parts
    }

    #[cfg(test)]
    pub(crate) fn latest_commit_file(&self) -> Option<&ParsedLogPath> {
        self.latest_commit_file.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn latest_crc_file(&self) -> Option<&ParsedLogPath> {
        self.latest_crc_file.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn ascending_compaction_files(&self) -> &[ParsedLogPath] {
        &self.ascending_compaction_files
    }

    #[cfg(test)]
    pub(crate) fn set_error(&mut self, error: String) {
        self.error = Some(error);
    }
}

//! Declarative plan node - tree structure for runtime interpretation.
//!
//! This enum provides a recursive tree representation of plans.
//! Engines that want generic plan execution can use this to walk the tree.

use super::nodes::*;

/// Declarative plan node - a recursive tree structure.
///
/// This enum allows engines to interpret plans at runtime without
/// knowing the specific composite plan type. Each variant wraps
/// either a leaf node or a wrapper containing child + node data.
///
/// # Example
///
/// ```ignore
/// fn execute(node: &DeclarativePlanNode) -> Result<Batches> {
///     match node {
///         DeclarativePlanNode::Scan(scan) => execute_scan(scan),
///         DeclarativePlanNode::Filter { child, node } => {
///             let input = execute(child)?;
///             apply_filter(input, node)
///         }
///         // ... other variants
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    // =========================================================================
    // Leaf Nodes (no children)
    // =========================================================================
    
    /// Scan files from storage
    Scan(ScanNode),
    
    /// List files from storage path
    FileListing(FileListingNode),
    
    /// Query parquet file schema (footer read only)
    SchemaQuery(SchemaQueryNode),

    // =========================================================================
    // Unary Nodes (one child)
    // =========================================================================
    
    /// Filter using kernel-defined function (KDF)
    FilterByKDF {
        child: Box<DeclarativePlanNode>,
        node: FilterByKDF,
    },
    
    /// Consume using kernel-defined function (KDF) - returns Continue/Break
    ///
    /// Unlike FilterByKDF which returns a per-row selection vector, ConsumeByKDF
    /// processes batches and returns a single boolean for control flow:
    /// - `true` = Continue (keep feeding data)
    /// - `false` = Break (stop iteration)
    ConsumeByKDF {
        child: Box<DeclarativePlanNode>,
        node: ConsumeByKDF,
    },
    
    /// Filter using predicate expression
    FilterByExpression {
        child: Box<DeclarativePlanNode>,
        node: FilterByExpressionNode,
    },
    
    /// Project/transform columns
    Select {
        child: Box<DeclarativePlanNode>,
        node: SelectNode,
    },
    
    /// Parse JSON column into structured data
    ParseJson {
        child: Box<DeclarativePlanNode>,
        node: ParseJsonNode,
    },
    
    /// Extract first non-null values
    FirstNonNull {
        child: Box<DeclarativePlanNode>,
        node: FirstNonNullNode,
    },

    // =========================================================================
    // Sink Nodes (terminal nodes that consume data)
    // =========================================================================

    /// Terminal sink node - consumes data flow.
    ///
    /// All complete plans must end with a sink. The sink type determines
    /// what happens to the data:
    /// - `Drop`: Data is consumed and discarded
    /// - `Results`: Data is streamed back to the user
    Sink {
        child: Box<DeclarativePlanNode>,
        node: SinkNode,
    },
}

impl DeclarativePlanNode {
    // =========================================================================
    // Builder methods for constructing plans
    // =========================================================================

    /// Create a scan node for parquet files.
    pub fn scan_parquet(files: Vec<crate::FileMeta>, schema: crate::schema::SchemaRef) -> Self {
        Self::Scan(ScanNode {
            file_type: FileType::Parquet,
            files,
            schema,
        })
    }

    /// Create a scan node for JSON files.
    pub fn scan_json(files: Vec<crate::FileMeta>, schema: crate::schema::SchemaRef) -> Self {
        Self::Scan(ScanNode {
            file_type: FileType::Json,
            files,
            schema,
        })
    }

    /// Add a kernel-defined function (KDF) filter with AddRemoveDedup state.
    pub fn filter_by_add_remove_dedup(self) -> Self {
        Self::FilterByKDF {
            child: Box::new(self),
            node: FilterByKDF::add_remove_dedup(),
        }
    }

    /// Add a kernel-defined function (KDF) filter with CheckpointDedup state.
    pub fn filter_by_checkpoint_dedup(self) -> Self {
        Self::FilterByKDF {
            child: Box::new(self),
            node: FilterByKDF::checkpoint_dedup(),
        }
    }

    /// Add a kernel-defined function (KDF) filter with existing typed state.
    pub fn filter_by_kdf_with_state(self, state: super::kdf_state::FilterKdfState) -> Self {
        Self::FilterByKDF {
            child: Box::new(self),
            node: FilterByKDF::with_state(state),
        }
    }

    /// Add a consumer KDF with LogSegmentBuilder state.
    ///
    /// The consumer will accumulate file listing results into a LogSegment.
    ///
    /// # Arguments
    /// * `log_root` - The log directory root URL
    /// * `end_version` - Optional end version to stop at
    /// * `checkpoint_hint_version` - Optional checkpoint hint version from `_last_checkpoint`
    pub fn consume_by_log_segment_builder(
        self,
        log_root: url::Url,
        end_version: Option<crate::Version>,
        checkpoint_hint_version: Option<crate::Version>,
    ) -> Self {
        Self::ConsumeByKDF {
            child: Box::new(self),
            node: ConsumeByKDF::log_segment_builder(log_root, end_version, checkpoint_hint_version),
        }
    }

    /// Add a consumer KDF with CheckpointHintReader state.
    ///
    /// The consumer will extract checkpoint hint information from scan results.
    pub fn consume_by_checkpoint_hint_reader(self) -> Self {
        Self::ConsumeByKDF {
            child: Box::new(self),
            node: ConsumeByKDF::checkpoint_hint_reader(),
        }
    }

    /// Add a consumer KDF with existing typed state.
    pub fn consume_by_kdf_with_state(self, state: super::kdf_state::ConsumerKdfState) -> Self {
        Self::ConsumeByKDF {
            child: Box::new(self),
            node: ConsumeByKDF::with_state(state),
        }
    }

    /// Add a predicate filter to this plan.
    pub fn filter_by_expr(self, predicate: std::sync::Arc<crate::Expression>) -> Self {
        Self::FilterByExpression {
            child: Box::new(self),
            node: FilterByExpressionNode { predicate },
        }
    }

    /// Add a projection to this plan.
    pub fn select(
        self,
        columns: Vec<std::sync::Arc<crate::Expression>>,
        output_schema: crate::schema::SchemaRef,
    ) -> Self {
        Self::Select {
            child: Box::new(self),
            node: SelectNode {
                columns,
                output_schema,
            },
        }
    }

    /// Add JSON parsing to this plan.
    pub fn parse_json(
        self,
        json_column: impl Into<String>,
        target_schema: crate::schema::SchemaRef,
        output_column: impl Into<String>,
    ) -> Self {
        Self::ParseJson {
            child: Box::new(self),
            node: ParseJsonNode {
                json_column: json_column.into(),
                target_schema,
                output_column: output_column.into(),
            },
        }
    }

    /// Add first-non-null extraction to this plan.
    pub fn first_non_null(self, columns: Vec<String>) -> Self {
        Self::FirstNonNull {
            child: Box::new(self),
            node: FirstNonNullNode { columns },
        }
    }

    // =========================================================================
    // Sink builder methods
    // =========================================================================

    /// Terminate plan with a Drop sink (discard data).
    ///
    /// The Drop sink consumes all incoming data and discards it.
    /// Useful for side-effect-only operations or internal sub-plans.
    pub fn sink_drop(self) -> Self {
        Self::Sink {
            child: Box::new(self),
            node: SinkNode::drop(),
        }
    }

    /// Terminate plan with a Results sink (stream to user).
    ///
    /// The Results sink marks data as user-facing results that
    /// should be streamed back to the caller.
    pub fn sink_results(self) -> Self {
        Self::Sink {
            child: Box::new(self),
            node: SinkNode::results(),
        }
    }

    // =========================================================================
    // Tree traversal helpers
    // =========================================================================

    /// Get the children of this node.
    pub fn children(&self) -> Vec<&DeclarativePlanNode> {
        match self {
            // Leaf nodes
            Self::Scan(_) | Self::FileListing(_) | Self::SchemaQuery(_) => vec![],
            // Unary nodes (including Sink)
            Self::FilterByKDF { child, .. }
            | Self::ConsumeByKDF { child, .. }
            | Self::FilterByExpression { child, .. }
            | Self::Select { child, .. }
            | Self::ParseJson { child, .. }
            | Self::FirstNonNull { child, .. }
            | Self::Sink { child, .. } => vec![child.as_ref()],
        }
    }

    /// Check if this is a leaf node (no children).
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Scan(_) | Self::FileListing(_) | Self::SchemaQuery(_))
    }

    /// Check if this plan is complete (ends with a sink).
    ///
    /// A plan is considered complete only if it terminates with a Sink node.
    /// This ensures explicit handling of output data fate.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Sink { .. })
    }

    // =========================================================================
    // Sink type inspection helpers
    // =========================================================================

    /// Get the sink type if this plan ends with a Sink node.
    ///
    /// Returns `Some(SinkType)` if this is a Sink node, `None` otherwise.
    /// Used by drivers to determine how to handle plan results.
    pub fn sink_type(&self) -> Option<SinkType> {
        match self {
            Self::Sink { node, .. } => Some(node.sink_type),
            _ => None,
        }
    }

    /// Check if this is a complete plan with a Results sink.
    ///
    /// Returns `true` if this plan ends with a Results sink that streams
    /// data back to the user.
    pub fn is_results_sink(&self) -> bool {
        self.sink_type() == Some(SinkType::Results)
    }

    /// Check if this is a complete plan with a Drop sink.
    ///
    /// Returns `true` if this plan ends with a Drop sink that discards
    /// data after processing (for side-effect-only operations).
    pub fn is_drop_sink(&self) -> bool {
        self.sink_type() == Some(SinkType::Drop)
    }
}


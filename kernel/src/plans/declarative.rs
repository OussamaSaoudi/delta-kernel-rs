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

    /// Add a kernel-defined function (KDF) filter to this plan.
    pub fn filter_by_kdf(self, function_id: FilterKernelFunctionId) -> Self {
        Self::FilterByKDF {
            child: Box::new(self),
            node: FilterByKDF {
                function_id,
                state_ptr: 0,
            },
        }
    }

    /// Add a kernel-defined function (KDF) filter with state pointer.
    pub fn filter_by_kdf_with_state(self, function_id: FilterKernelFunctionId, state_ptr: u64) -> Self {
        Self::FilterByKDF {
            child: Box::new(self),
            node: FilterByKDF {
                function_id,
                state_ptr,
            },
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
    // Tree traversal helpers
    // =========================================================================

    /// Get the children of this node.
    pub fn children(&self) -> Vec<&DeclarativePlanNode> {
        match self {
            // Leaf nodes
            Self::Scan(_) | Self::FileListing(_) | Self::SchemaQuery(_) => vec![],
            // Unary nodes
            Self::FilterByKDF { child, .. }
            | Self::FilterByExpression { child, .. }
            | Self::Select { child, .. }
            | Self::ParseJson { child, .. }
            | Self::FirstNonNull { child, .. } => vec![child.as_ref()],
        }
    }

    /// Check if this is a leaf node (no children).
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Scan(_) | Self::FileListing(_) | Self::SchemaQuery(_))
    }
}


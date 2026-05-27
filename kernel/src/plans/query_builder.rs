//! Builder API for constructing a [`ResultPlan`] over a single relational node.
//!
//! The builder produces single-node [`Plan`]s today (one scan source, no transforms);
//! it will grow to support multi-node plans. Until then, multi-node [`Plan`]s are
//! constructed directly via [`Plan`] / [`PlanNode`].

use super::ir::nodes::{NodeKind, ScanJsonNode, ScanParquetNode};
use super::ir::plan::{Plan, PlanNode, RefId, ResultPlan};
use crate::schema::SchemaRef;
use crate::{DeltaResult, FileMeta};

/// Builder for constructing a single-node [`ResultPlan`].
#[derive(Debug)]
pub struct QueryPlanBuilder {
    kind: NodeKind,
}

impl QueryPlanBuilder {
    /// Construct a [`ScanJsonNode`] over the given files.
    ///
    /// See [`ScanJsonNode`] for parameter semantics.
    pub fn scan_json(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            kind: NodeKind::ScanJson(ScanJsonNode { files, schema }),
        }
    }

    /// Construct a [`ScanParquetNode`] over the given files.
    ///
    /// See [`ScanParquetNode`] for parameter semantics.
    pub fn scan_parquet(files: Vec<FileMeta>, schema: SchemaRef) -> Self {
        Self {
            kind: NodeKind::ScanParquet(ScanParquetNode { files, schema }),
        }
    }

    /// Consume the builder and produce a [`ResultPlan`] with a single node whose output is
    /// `RefId(0)`.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns `DeltaResult` for a stable signature as the builder
    /// grows multi-node validation in follow-up work.
    pub fn build(self) -> DeltaResult<ResultPlan> {
        let output = RefId(0);
        let node = PlanNode {
            kind: self.kind,
            inputs: vec![],
            output,
        };
        Ok(ResultPlan {
            plan: Plan { nodes: vec![node] },
            result: output,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::schema::{DataType, StructField, StructType};
    use crate::FileMeta;

    fn test_schema() -> SchemaRef {
        Arc::new(StructType::new_unchecked([StructField::not_null(
            "id",
            DataType::LONG,
        )]))
    }

    fn test_file(path: &str) -> FileMeta {
        FileMeta {
            location: Url::parse(path).unwrap(),
            last_modified: 0,
            size: 0,
        }
    }

    enum Format {
        Json,
        Parquet,
    }

    #[rstest]
    #[case::json(Format::Json, &["file:///a.json", "file:///b.json"])]
    #[case::parquet(Format::Parquet, &["file:///a.parquet", "file:///b.parquet"])]
    fn build_constructs_scan_node(#[case] format: Format, #[case] urls: &[&str]) {
        let schema = test_schema();
        let files: Vec<FileMeta> = urls.iter().map(|u| test_file(u)).collect();

        let ResultPlan { plan, result } = match format {
            Format::Json => QueryPlanBuilder::scan_json(files.clone(), schema.clone()),
            Format::Parquet => QueryPlanBuilder::scan_parquet(files.clone(), schema.clone()),
        }
        .build()
        .unwrap();

        let [node] = <[_; 1]>::try_from(plan.nodes).expect("single-node plan");
        assert_eq!(node.output, result);
        let (NodeKind::ScanJson(ScanJsonNode {
            files: scan_files,
            schema: node_schema,
        })
        | NodeKind::ScanParquet(ScanParquetNode {
            files: scan_files,
            schema: node_schema,
        })) = node.kind
        else {
            panic!("expected ScanJson / ScanParquet, got {:?}", node.kind);
        };
        assert_eq!(scan_files, files);
        assert!(Arc::ptr_eq(&node_schema, &schema));
    }
}

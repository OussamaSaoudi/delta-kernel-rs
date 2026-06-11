//! Plan node operator kinds and their payloads.
//!
//! [`NodeKind`] enumerates every operator. Each operator's payload struct is defined below.

use std::sync::Arc;

use url::Url;

use crate::expressions::{ColumnName, Expression, Predicate, PredicateRef, Scalar};
use crate::plans::kernel_reducers::{KernelReducer, KernelReducerToken, ReducerHandle};
use crate::schema::SchemaRef;
use crate::FileMeta;

// ============================================================================
// NodeKind -- enumerates every operator kind
// ============================================================================

/// Plan node operator kinds.
///
/// Sources take zero inputs; unary operators take one; binary operators take two;
/// n-ary operators take a variable number. Output schemas are stored on the payload
/// struct for operators whose caller declares them (`ScanParquet`, `ScanJson`,
/// `Values`, `Load`, `Project`, `MaxByVersion`); for the rest the engine derives
/// the output schema from inputs and parameters.
///
/// Note: `ListFiles` is a prototype-only source that emits a file-listing stream;
/// the upstream c-stack IR does not include it.
#[derive(Debug, Clone)]
pub enum NodeKind {
    // === Source operators (0 inputs) =========================================
    /// Prototype-only: emit a file-listing stream from a storage prefix. Not part of the
    /// upstream c-stack IR; used by the scan state machine for log-file enumeration.
    ListFiles(ListFilesNode),
    ScanParquet(ScanParquetNode),
    ScanJson(ScanJsonNode),
    Values(ValuesNode),

    // === Unary operators (1 input) ===========================================
    Project(ProjectNode),
    Filter(FilterNode),
    Load(LoadNode),
    MaxByVersion(MaxByVersionNode),

    // === Binary operators (2 inputs) =========================================
    EquiJoin(EquiJoinNode),

    // === N-ary operators (variable inputs) ===================================
    UnionAll(UnionAllNode),
}

impl std::fmt::Display for NodeKind {
    /// Writes the variant name (e.g. `ScanParquet`, `Project`) without its payload.
    /// Useful for error messages and logs where the operator's identity matters but
    /// the parameters don't.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            NodeKind::ListFiles(_) => "ListFiles",
            NodeKind::ScanParquet(_) => "ScanParquet",
            NodeKind::ScanJson(_) => "ScanJson",
            NodeKind::Values(_) => "Values",
            NodeKind::Project(_) => "Project",
            NodeKind::Filter(_) => "Filter",
            NodeKind::Load(_) => "Load",
            NodeKind::MaxByVersion(_) => "MaxByVersion",
            NodeKind::EquiJoin(_) => "EquiJoin",
            NodeKind::UnionAll(_) => "UnionAll",
        })
    }
}

// ============================================================================
// Source operators (0 inputs)
// ============================================================================

/// Prototype-only source. Recursively lists files under the directory derived from
/// `start_from`. Results are streamed as one row per file, sorted by full path in UTF-8
/// lexicographic byte order.
///
/// # Listing semantics
///
/// `start_from` is interpreted as either a directory cursor or a file cursor:
///
/// - **Directory-like** (`start_from` ends with `/`): the listing's parent directory is
///   `start_from` itself, and every file at or below it is emitted (inclusive).
/// - **File-like** (no trailing `/`): the parent directory is `start_from`'s containing directory,
///   and only files whose full path sorts **strictly greater** than `start_from` are emitted
///   (exclusive). This makes it natural to resume a paged listing by passing the last-seen path
///   back in.
///
/// The listing is recursive: files in nested subdirectories are included, interleaved
/// with files at shallower depths in lexicographic order. For example, listing from
/// `dir/0001.json` (file-like) may yield `dir/0002.json`, `dir/sub/0003.json`,
/// `dir/sub/nested/0004.json`, ... all interleaved by full-path order.
///
/// # Output schema
///
/// Each emitted row carries:
///
/// - `path` (string, non-null) -- the fully qualified URL.
/// - `size` (long, non-null) -- size in bytes.
/// - `modificationTime` (long, non-null) -- last-modified time, milliseconds since the Unix epoch.
///
/// Mirrors the engine's [`StorageHandler::list_from`] semantics; the kernel does not
/// perform I/O directly.
///
/// [`StorageHandler::list_from`]: crate::StorageHandler::list_from
#[derive(Debug, Clone)]
pub struct ListFilesNode {
    pub start_from: Url,
}

/// Reads Parquet `files` into row batches matching `schema`.
///
/// The engine emits rows file-by-file in the order `files` is given. Within a file
/// rows stay in file order, and no batch crosses file boundaries.
///
/// Each `schema` field is matched to a Parquet column by
/// [field ID](crate::schema::ColumnMetadataKey::ParquetFieldId) when set on the field,
/// falling back to column name. Missing columns produce NULL for nullable fields and
/// an error for non-nullable fields. Output columns appear in `schema`
/// declaration order regardless of the Parquet file's physical layout.
///
/// Metadata columns declared via [`StructField::create_metadata_column`] are populated
/// by the engine rather than read from the file:
/// [`MetadataColumnSpec::RowIndex`] supplies the 0-based row position within the
/// current file (`LONG`, non-null), and [`MetadataColumnSpec::FilePath`] supplies the
/// file's URL (`STRING`, non-null).
///
/// `predicate`, when `Some`, is a push-down hint the engine may use to skip data
/// (row-group / page-index pruning) but is free to ignore. Output may include rows the
/// predicate would reject; callers needing strict filtering should add a downstream
/// [`FilterNode`]. `None` means no hint.
///
/// [`StructField::create_metadata_column`]: crate::schema::StructField::create_metadata_column
/// [`MetadataColumnSpec::RowIndex`]: crate::schema::MetadataColumnSpec::RowIndex
/// [`MetadataColumnSpec::FilePath`]: crate::schema::MetadataColumnSpec::FilePath
#[derive(Debug, Clone)]
pub struct ScanParquetNode {
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
    pub predicate: Option<PredicateRef>,
}

/// Reads newline-delimited JSON `files` (one JSON object per line) into row batches
/// matching `schema`.
///
/// The engine emits rows file-by-file in the order `files` is given. Within a file
/// rows stay in file order, and no batch crosses file boundaries. Missing fields in
/// a row produce NULL for nullable `schema` fields and an error for
/// non-nullable fields.
///
/// `predicate`, when `Some`, is a push-down hint the engine may use to skip rows but
/// is free to ignore. Line-oriented JSON readers have no column-index equivalent of
/// Parquet pushdown, so engines typically either evaluate post-decode or skip the hint
/// entirely. Output may include rows the predicate would reject; callers needing
/// strict filtering should add a downstream [`FilterNode`]. `None` means no hint.
#[derive(Debug, Clone)]
pub struct ScanJsonNode {
    pub files: Vec<FileMeta>,
    pub schema: SchemaRef,
    pub predicate: Option<PredicateRef>,
}

/// Inline literal rows. Each `rows[i]` has one [`Scalar`] per field in `schema`, in
/// field order; `rows[i].len() == schema.fields().count()` for every row.
///
/// # Example
///
/// Two rows over `{ id: int, active: bool }`:
///
/// ```text
/// ValuesNode {
///     schema: { id: int, active: bool },
///     rows: [
///         [1, true],
///         [2, false],
///     ],
/// }
/// ```
///
/// produces:
///
/// ```text
/// id | active
/// ---+--------
///  1 |  true
///  2 | false
/// ```
#[derive(Debug, Clone)]
pub struct ValuesNode {
    pub schema: SchemaRef,
    pub rows: Vec<Vec<Scalar>>,
}

// ============================================================================
// Unary operators (1 input)
// ============================================================================

/// Projects the single input through `named_exprs` into rows of `output_schema`.
///
/// `named_exprs.len() == output_schema.fields().count()`: for each output field `i`,
/// the engine evaluates `named_exprs[i].1` against an input row and binds the value
/// to the field named by `named_exprs[i].0`, which matches `output_schema.fields()[i].name`.
/// Engines compile against `output_schema` directly and do not re-derive it from the
/// expressions.
///
/// # Example
///
/// Projecting an input `{ id: int, first: string, last: string }` to
/// `{ id: int, name: string }` by renaming and concatenating:
///
/// ```text
/// ProjectNode {
///     named_exprs: [
///         ("id",   col("id")),
///         ("name", concat(col("first"), " ", col("last"))),
///     ],
///     output_schema: { id: int, name: string },
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ProjectNode {
    pub named_exprs: Vec<(String, Arc<Expression>)>,
    pub output_schema: SchemaRef,
}

/// Keeps input rows where `predicate` evaluates true (SQL null semantics).
/// Output schema is the input schema unchanged.
#[derive(Debug, Clone)]
pub struct FilterNode {
    pub predicate: Arc<Predicate>,
}

// === Load ===================================================================

/// File formats supported by [`LoadNode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Parquet,
    Json,
}

/// Column names a [`LoadNode`] reads from each upstream row to resolve which file to
/// open. `path_column` is required; `file_size_column` and `num_records_column` are
/// optional and used by engines as split-sizing / pruning hints.
///
/// All three are [`ColumnName`]s and may reference nested fields (e.g. `add.path` on
/// a Delta-checkpoint upstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadColumnInfo {
    /// Column on the upstream relation holding the per-row file path /
    /// URL fragment. Joined to [`LoadNode::base_url`] when set.
    pub path_column: ColumnName,
    /// Optional column with the file's total size in bytes.
    pub file_size_column: Option<ColumnName>,
    /// Optional column with the file's row-count (parquet-encoded `numRecords`).
    pub num_records_column: Option<ColumnName>,
}

/// Encoding of a deletion-vector column referenced by [`DvRef`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DvKind {
    /// Column holds the raw RoaringTreemap bitmap bytes for the DV.
    Bytes,
    /// Column holds a [`DeletionVectorDescriptor`] struct that the engine resolves into a
    /// bitmap before applying.
    ///
    /// [`DeletionVectorDescriptor`]: crate::actions::deletion_vector::DeletionVectorDescriptor
    Descriptor,
}

/// Deletion-vector reference attached to a [`LoadNode`]. Rows whose row index appears in
/// the DV are dropped from the file read; `kind` selects how to interpret the column
/// value (raw bitmap bytes vs. a descriptor struct).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DvRef {
    pub column: ColumnName,
    pub kind: DvKind,
}

/// Reads data files identified by an upstream stream of file-metadata tuples. Each
/// input row describes one file. `file_meta.path_column` names the path column on the
/// upstream relation (mandatory); `file_meta.file_size_column` and
/// `file_meta.num_records_column` name the file's size and row-count columns (optional;
/// engines use them as split-sizing hints). The engine resolves each path against
/// `base_url`, opens the file as `file_type`, and reads columns matching `file_schema`
/// from it.
///
/// `metadata_derived_columns` lists columns on the upstream row whose values are
/// broadcast onto every emitted file row (e.g. `version` for table-changes scans, or
/// partition values).
///
/// `dv_ref`, when set, applies a per-row deletion-vector mask: rows whose row index
/// is present in the DV are dropped from the file's output. The DV column may hold
/// either a Delta [`DeletionVectorDescriptor`] struct that the engine resolves into a
/// bitmap, or a raw RoaringTreemap bitmap byte buffer; [`DvKind`] selects between the
/// two encodings.
///
/// [`DeletionVectorDescriptor`]: crate::actions::deletion_vector::DeletionVectorDescriptor
///
/// Each upstream path is resolved against `base_url`:
///
/// - **`Some(base)`**: each path column value is treated as a path relative to `base` and resolved
///   via [`Url::join`]. Paths that are themselves absolute URLs (any scheme prefix) bypass the join
///   and are used as-is, matching the Delta protocol convention for `Add.path`
///   (relative-to-table-root OR a fully-qualified URI).
/// - **`None`**: every path column value must already be an absolute URL; the engine errors on
///   relative paths.
///
/// # Example
///
/// Given an upstream metadata stream with two rows (Parquet `file_schema` is
/// `{ id: int, name: string }`, `metadata_derived_columns = [version]`,
/// `base_url = s3://table/`):
///
/// ```text
/// upstream (metadata)
///     path             | size | version
///     -----------------+------+---------
///     part-0.parquet   | 1024 |       7
///     part-1.parquet   | 2048 |       8
/// ```
///
/// The engine opens `s3://table/part-0.parquet` and `s3://table/part-1.parquet`, reads
/// `{id, name}` from each, and broadcasts the row's `version` onto every emitted file
/// row:
///
/// ```text
/// output (file rows)
///     id | name | version
///     ---+------+---------
///      1 |  a   |       7    <- from part-0.parquet
///      2 |  b   |       7
///      3 |  c   |       8    <- from part-1.parquet
///      4 |  d   |       8
/// ```
#[derive(Debug, Clone)]
pub struct LoadNode {
    pub file_schema: SchemaRef,
    pub file_type: FileType,
    pub base_url: Option<Url>,
    pub metadata_derived_columns: Vec<ColumnName>,
    pub file_meta: LoadColumnInfo,
    pub dv_ref: Option<DvRef>,
}

// === MaxByVersion ===========================================================

/// "Top 1 per group, ordered by version desc" -- a specialized aggregate. Emitted rows
/// match `output_schema`: each field name selects a column from the winning input row,
/// and the field's declared type must match that column's type in the input. Group-by
/// expressions and the version column are not implicitly projected -- include them in
/// `output_schema` if the caller wants them in the output.
/// 
/// TODO: Document that this can benefit from ordered input. You should be able to do 
/// topK in a performant manner.
///
/// Equivalent SQL:
///
/// ```sql
/// SELECT <output_schema fields>
/// FROM (
///     SELECT *,
///            ROW_NUMBER() OVER (
///                PARTITION BY <group_by>
///                ORDER BY <version_column> DESC
///            ) AS rn
///     FROM input
/// ) WHERE rn = 1
/// ```
///
/// Ties (multiple input rows with the same group keys AND the same version value) are
/// broken by input order: the first such row encountered wins.
#[derive(Debug, Clone)]
pub struct MaxByVersionNode {
    pub group_by: Vec<Arc<Expression>>,
    pub version_column: Arc<Expression>,
    pub output_schema: SchemaRef,
}

// ============================================================================
// Binary operators (2 inputs)
// ============================================================================

/// Equi-join semantics. Each variant documents which rows it emits and the resulting
/// output schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    /// Emit each left row whose key matches no right row. Right rows contribute
    /// nothing to the output; the output schema equals the left input schema.
    LeftAnti,
}

/// Equi-join two inputs (`inputs.len() == 2`, convention `[left, right]`).
/// `left_keys[i]` is matched against `right_keys[i]` for each `i`; the two vectors
/// must have the same length, which the builder enforces.
///
/// # Example
///
/// `LeftAnti` join "keep `left` rows whose `path` does not appear in `right`":
///
/// ```text
/// EquiJoinNode {
///     kind: JoinKind::LeftAnti,
///     left_keys:  [col("path")],
///     right_keys: [col("path")],
/// }
///
/// left                right
/// path | version      path
/// -----+--------      ----
///  a   |   1           b
///  b   |   2           d
///  c   |   3
///
/// output (left rows whose path is not in right):
/// path | version
/// -----+--------
///  a   |   1
///  c   |   3
/// ```
#[derive(Debug, Clone)]
pub struct EquiJoinNode {
    pub kind: JoinKind,
    pub left_keys: Vec<Arc<Expression>>,
    pub right_keys: Vec<Arc<Expression>>,
}

// ============================================================================
// N-ary operators (variable inputs)
// ============================================================================

/// Concatenates N inputs (`inputs.len() >= 1`). All input schemas must agree.
/// `ordered=true` preserves child order; `ordered=false` permits reordering.
///
/// # Example
///
/// `UnionAllNode { ordered: true }` over two inputs with schema `{ id: int }`:
///
/// ```text
/// input 0       input 1
/// id            id
/// --            --
///  1             3
///  2             4
///
/// output (ordered=true preserves child order):
/// id
/// --
///  1
///  2
///  3
///  4
/// ```
///
/// With `ordered=false` the engine may interleave or reorder the inputs' rows.
#[derive(Debug, Clone)]
pub struct UnionAllNode {
    pub ordered: bool,
}

// ============================================================================
// Reducer-drain sink (referenced by `EngineRequest::Reduce`)
// ============================================================================

/// Template for draining a row stream into a [`KernelReducer`] via [`EngineRequest::Reduce`].
///
/// - `initial_state`: cloned per partition via [`DynClone`](dyn_clone::DynClone) into a
///   [`ReducerHandle`].
/// - `token`: keys the finished handle returned from the executor and validated at decode time by
///   the paired [`Extractor`].
///
/// [`EngineRequest::Reduce`]: crate::plans::state_machines::framework::state_machine::EngineRequest::Reduce
/// [`Extractor`]: crate::plans::kernel_reducers::Extractor
#[derive(Debug, Clone)]
pub struct ReduceSink {
    pub initial_state: Box<dyn KernelReducer>,
    pub token: KernelReducerToken,
}

impl ReduceSink {
    /// Construct from a concrete reducer and mint a fresh token from its `kind`.
    pub fn new_reducer<R: KernelReducer + 'static>(state: R) -> Self {
        let token = KernelReducerToken::new(state.kind());
        Self {
            initial_state: Box::new(state),
            token,
        }
    }

    /// Mint a runtime [`ReducerHandle`] for this sink template by cloning the initial state.
    pub fn new_handle(&self) -> ReducerHandle {
        ReducerHandle::new(self.token.clone(), self.initial_state.clone())
    }
}

// Token identity drives equality: tokens are process-unique by id, and the
// `initial_state` trait object (`Box<dyn KernelReducer>`) is not `Eq`-able. Two
// sinks sharing a token were constructed from the same plan node and therefore
// describe the same reducer.
impl PartialEq for ReduceSink {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
    }
}

impl Eq for ReduceSink {}

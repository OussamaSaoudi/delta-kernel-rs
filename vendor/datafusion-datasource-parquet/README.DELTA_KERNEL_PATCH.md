# delta-kernel-rs vendor patch: datafusion-datasource-parquet

Baseline: Apache DataFusion **53.1.0** `datafusion-datasource-parquet` (crates.io).

## Delta Kernel additions

1. **`ParquetSource::with_virtual_columns`** — additive builder forwarding virtual Arrow fields
   (validated by arrow-rs the same way as `ArrowReaderOptions::with_virtual_columns`).
2. **`ParquetOpener::virtual_columns`** — merges those fields into the initial
   `ArrowReaderOptions` before metadata load so decoded batches include native virtual columns
   (e.g. parquet `RowNumber`).
3. **Projection mask filtering** — logical projection indices may reference Arrow virtual columns at
   the end of the merged schema; `ParquetOpener` drops indices beyond parquet root columns before
   `ProjectionMask::roots` **unless** virtual columns are configured — then it uses `ProjectionMask::all`
   so arrow-rs retains synthesized columns through decode (narrow-only masks strip virtual outputs).

4. **Parquet root field-ID-aware adaptation** — when the logical file schema carries Arrow
   `PARQUET:field_id` metadata on any **top-level** field, `ParquetOpener` clones the decoded physical
   schema but renames roots to logical scan names using ID-first matching with name fallback (Delta
   Kernel semantics for flat reads). Decoder stream schemas keep physical names; they are temporarily
   aligned before `reassign_expr_columns` so projection reordering stays consistent.

Default remains unchanged: empty virtual columns list; absent logical field IDs preserve upstream name-only matching.

## Upstream alignment

Tracks Apache DataFusion issue **#20132** / PR **#20133** (virtual columns in the parquet scan).
When upstream lands an equivalent API, drop this vendor directory and remove the workspace
`[patch.crates-io]` entry after bumping the crates.io version.

## Risks

- **Drift**: vendor snapshot must be rebased when bumping DataFusion 53.x patch releases.
- **Schema contracts**: callers must extend `TableSchema` / projection so logical schemas match
  arrow-rs virtual columns appended by the decoder.

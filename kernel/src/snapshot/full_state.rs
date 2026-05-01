//! Declarative full snapshot read (FSR) scaffolding for [`Snapshot`].
//!
//! Phase 4.1 wires snapshot log-segment shape into the FSR strip/fanout coroutine. Engines such as
//! the DataFusion-backed executor drive the returned [`CoroutineSM`]. Classic
//! [`crate::scan::ScanBuilder`] replay is unchanged when this module is not used.

use std::sync::Arc;

use crate::expressions::Scalar;
use crate::plans::errors::DeltaErrAsKernel;
use crate::plans::state_machines::framework::coroutine::engine::CoroutineSM;
use crate::plans::state_machines::fsr::{
    try_build_fsr_strip_then_fanout_sm, FsrStripThenFanoutOutcome,
};
use crate::schema::{DataType, StructField, StructType};
use crate::{DeltaResult, Snapshot};

impl Snapshot {
    /// Declarative **full snapshot read** entry: a two-phase FSR [`CoroutineSM`] derived from this
    /// snapshot's log listing.
    ///
    /// # What it models
    ///
    /// Literal plan phases stand in for future checkpoint materialization and tail replay:
    ///
    /// - **Strip** — one row per checkpoint *part* in [`crate::log_segment::LogSegment`], or a
    ///   single sentinel row when the segment has no checkpoint.
    /// - **Fanout** — one row per tail *commit* file still present after the checkpoint strip, or a
    ///   single sentinel row when that set is empty.
    ///
    /// # Feature gate
    ///
    /// Available only with the `declarative-plans` feature. There is **no** runtime fallback: if
    /// the feature is disabled at compile time, callers rely on [`Snapshot::scan_builder`] and
    /// classic kernel replay instead.
    ///
    /// # Limitations
    ///
    /// Does not perform storage I/O or produce active data files — only exercises FSR-shaped
    /// multi-phase scheduling. Real reads remain on the scan/log-replay path until later phases
    /// swap literals for declarative checkpoint and file scans.
    pub fn full_state(&self) -> DeltaResult<CoroutineSM<FsrStripThenFanoutOutcome>> {
        let listed = &self.log_segment().listed;
        let n_cp_parts = listed.checkpoint_parts.len();
        let n_commits = listed.ascending_commit_files.len();

        let strip_schema = Arc::new(StructType::try_new([StructField::not_null(
            "checkpoint_part_ord",
            DataType::LONG,
        )])?);
        let strip_rows: Vec<Vec<Scalar>> = if n_cp_parts == 0 {
            vec![vec![Scalar::Long(-1)]]
        } else {
            (0..n_cp_parts)
                .map(|i| vec![Scalar::Long(i as i64)])
                .collect()
        };

        let fan_schema = Arc::new(StructType::try_new([StructField::not_null(
            "commit_tail_ord",
            DataType::LONG,
        )])?);
        let fanout_rows: Vec<Vec<Scalar>> = if n_commits == 0 {
            vec![vec![Scalar::Long(-1)]]
        } else {
            (0..n_commits)
                .map(|i| vec![Scalar::Long(i as i64)])
                .collect()
        };

        try_build_fsr_strip_then_fanout_sm(strip_schema, strip_rows, fan_schema, fanout_rows)
            .map_err(|e| e.into_kernel_default())
    }
}

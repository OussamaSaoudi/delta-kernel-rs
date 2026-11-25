//! The CRC a [`Snapshot`] holds, and the version semantics gating how it may be used.
//!
//! [`Snapshot`]: super::Snapshot

use crate::crc::{CrcDelta, CrcWithProvenance};
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// How the in-memory checksum relates to an observed checksum file and the snapshot version.
///
/// Connectors reporting checksum availability can group the variants as follows:
/// - Usable checksum at the snapshot version: [`AtVersion`](Self::AtVersion) and
///   [`Reconstructed`](Self::Reconstructed).
/// - No usable checksum resolved at the snapshot version: [`Stale`](Self::Stale),
///   [`Missing`](Self::Missing), and [`Unusable`](Self::Unusable). Each variant describes only the
///   versions loading considered.
/// - Not established: [`Unknown`](Self::Unknown).
///
/// The value records observations made while resolving the snapshot, not the current contents of
/// storage. An in-memory CRC at the snapshot version does not establish that a checksum file exists
/// on disk at that version.
///
/// The source is independent of table-level file statistics: a snapshot can have a usable checksum
/// that lacks complete file statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrcSource {
    /// A usable on-disk checksum file was resolved at the snapshot version.
    AtVersion {
        /// Version of both the checksum file and the in-memory CRC, equal to the snapshot version.
        crc_version: Version,
    },
    /// An older on-disk checksum was advanced in memory to the snapshot version using committed
    /// actions.
    Reconstructed {
        /// Version of the original on-disk checksum, preserved across in-memory updates.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: Version,
        /// Version of the in-memory CRC, equal to the snapshot version.
        crc_version: Version,
    },
    /// A usable in-memory checksum is older than the snapshot version.
    /// It may already incorporate commits replayed after its original on-disk version.
    Stale {
        /// Version of the on-disk checksum from which the in-memory CRC was read or reconstructed.
        /// Subtract this from `crc_version` to obtain the cumulative number of incorporated
        /// commits.
        on_disk_version: Version,
        /// Version of the in-memory CRC, strictly less than the snapshot version.
        /// Subtract this from the snapshot version to obtain the remaining version gap.
        crc_version: Version,
    },
    /// Loading found no checksum candidate in its listed version range and kept no in-memory
    /// CRC. Fresh loads consider the selected checkpoint version (or zero without a checkpoint)
    /// through the snapshot version, inclusive. Incremental loads may list only after the previous
    /// checkpoint, or after the previous snapshot when new checkpoints are ignored. Files below
    /// the selected checkpoint are not candidates. This does not describe unlisted versions.
    Missing,
    /// An observed checksum candidate was unusable and no usable baseline was resolved.
    /// This includes read failures as well as invalid checksum contents.
    Unusable,
    /// No observation of an on-disk checksum is available. Examples include snapshots constructed
    /// directly by CREATE TABLE without a checksum-file lookup, connector hints without
    /// provenance, and derived snapshots that discard a CRC at a checkpoint. It does not indicate
    /// that loading searched for a checksum and failed to find or read it.
    Unknown,
}

/// How CRC resolution relates to a particular snapshot.
///
/// Only current CRCs can answer authoritative queries. Stale CRCs remain replay bases; lookup
/// outcomes have no payload. Individual CRC provenance stays on the wrapped value.
#[derive(Debug, Clone)]
pub(crate) enum SnapshotCrc {
    /// The CRC describes this snapshot's version.
    Current(CrcWithProvenance),
    /// The CRC predates this snapshot and is available only as a replay base.
    Stale(CrcWithProvenance),
    /// No checksum candidate was found in the listed range.
    Missing,
    /// An observed checksum candidate could not provide a usable baseline.
    Unusable,
    /// No payload or applicable checksum-file observation is available.
    NotLookedUp,
}

impl SnapshotCrc {
    /// Classifies an absent lookup using the version range's checksum-file observation.
    pub(crate) fn from_lookup(crc: Option<CrcWithProvenance>, candidate_listed: bool) -> Self {
        match crc {
            Some(crc) => Self::Current(crc),
            None if candidate_listed => Self::Unusable,
            None => Self::Missing,
        }
    }

    /// Validates CRC bounds and classifies it relative to the snapshot version.
    pub(crate) fn try_new(
        crc: impl Into<Self>,
        snapshot_version: Version,
        checkpoint_version: Option<Version>,
    ) -> KernelResult<Self> {
        let state = crc.into();
        let Some(crc) = state.base() else {
            return Ok(state);
        };
        if let Some(on_disk_version) = crc.on_disk_version() {
            require!(
                on_disk_version <= crc.version(),
                KernelError::internal_error(format!(
                    "On-disk CRC version {on_disk_version} is ahead of retained CRC version {}",
                    crc.version()
                ))
            );
        }
        require!(
            crc.version() <= snapshot_version,
            KernelError::internal_error(format!(
                "CRC version {} is ahead of snapshot version {snapshot_version}",
                crc.version()
            ))
        );
        require!(
            checkpoint_version.is_none_or(|checkpoint| crc.version() >= checkpoint),
            KernelError::internal_error(format!(
                "CRC version {} is below checkpoint version {checkpoint_version:?}",
                crc.version()
            ))
        );
        Ok(match state {
            Self::Current(crc) | Self::Stale(crc) if crc.version() == snapshot_version => {
                Self::Current(crc)
            }
            Self::Current(crc) | Self::Stale(crc) => Self::Stale(crc),
            absent => absent,
        })
    }

    /// Public checksum-file telemetry, flattened from snapshot state and CRC provenance.
    pub(crate) fn source(&self) -> CrcSource {
        match self {
            Self::Current(crc) | Self::Stale(crc) => {
                let Some(on_disk_version) = crc.on_disk_version() else {
                    return CrcSource::Unknown;
                };
                let crc_version = crc.version();
                match self {
                    Self::Stale(_) => CrcSource::Stale {
                        on_disk_version,
                        crc_version,
                    },
                    _ if on_disk_version < crc_version => CrcSource::Reconstructed {
                        on_disk_version,
                        crc_version,
                    },
                    _ => CrcSource::AtVersion { crc_version },
                }
            }
            Self::Missing => CrcSource::Missing,
            Self::Unusable => CrcSource::Unusable,
            Self::NotLookedUp => CrcSource::Unknown,
        }
    }

    /// The CRC only when its version matches this snapshot's version.
    pub(crate) fn at_version(&self) -> Option<&CrcWithProvenance> {
        match self {
            Self::Current(crc) => Some(crc),
            _ => None,
        }
    }

    /// The in-memory CRC, including a stale baseline.
    pub(crate) fn base(&self) -> Option<&CrcWithProvenance> {
        match self {
            Self::Current(crc) | Self::Stale(crc) => Some(crc),
            _ => None,
        }
    }

    /// Applies a committed delta only to a current CRC.
    pub(crate) fn apply_commit(&self, delta: CrcDelta, version: Version) -> Self {
        match self {
            Self::Current(crc) => Self::Current(crc.apply(delta, version)),
            _ => self.clone(),
        }
    }

    /// Omits stale CRCs when deriving hints and checkpoints.
    pub(crate) fn only_current(&self) -> Self {
        match self {
            Self::Stale(_) => Self::NotLookedUp,
            _ => self.clone(),
        }
    }
}

impl From<CrcWithProvenance> for SnapshotCrc {
    fn from(crc: CrcWithProvenance) -> Self {
        Self::Current(crc)
    }
}

impl From<Option<CrcWithProvenance>> for SnapshotCrc {
    fn from(crc: Option<CrcWithProvenance>) -> Self {
        crc.map_or(Self::NotLookedUp, Self::Current)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_utils::assert_result_error_with_message;

    use super::*;
    use crate::crc::{Crc, CrcDelta, FileStats, FileStatsState};

    fn crc_at(version: Version) -> Arc<Crc> {
        Arc::new(Crc {
            version,
            file_stats_state: FileStatsState::Complete(FileStats {
                num_files: 1,
                table_size_bytes: 100,
                file_size_histogram: None,
            }),
            ..Default::default()
        })
    }

    #[rstest::rstest]
    #[case::ahead_of_snapshot(6, None, None, "is ahead of snapshot version")]
    #[case::below_checkpoint(2, Some(3), None, "is below checkpoint version")]
    #[case::ahead_of_crc(5, None, Some(6), "is ahead of retained CRC version")]
    fn new_rejects_invalid_argument_combinations(
        #[case] crc_version: Version,
        #[case] checkpoint_version: Option<Version>,
        #[case] on_disk_version: Option<Version>,
        #[case] expected_error: &str,
    ) {
        let crc = CrcWithProvenance::from_snapshot_hint(crc_at(crc_version), on_disk_version);
        assert_result_error_with_message(
            SnapshotCrc::try_new(crc, 5, checkpoint_version),
            expected_error,
        );
    }

    #[rstest::rstest]
    #[case::at_checkpoint(3, Some(3), false)]
    #[case::at_snapshot(5, None, true)]
    #[case::stale(3, None, false)]
    fn authoritative_access_is_independent_of_file_provenance(
        #[case] version: Version,
        #[case] checkpoint: Option<Version>,
        #[case] current: bool,
    ) {
        let crc = CrcWithProvenance::from_snapshot_hint(crc_at(version), None);
        let holder = SnapshotCrc::try_new(crc, 5, checkpoint).unwrap();
        assert_eq!(holder.base().map(|crc| crc.version()), Some(version));
        assert_eq!(holder.at_version().is_some(), current);
        assert_eq!(holder.source(), CrcSource::Unknown);
    }

    #[rstest::rstest]
    #[case::missing(SnapshotCrc::Missing, CrcSource::Missing)]
    #[case::unusable(SnapshotCrc::Unusable, CrcSource::Unusable)]
    #[case::not_looked_up(SnapshotCrc::NotLookedUp, CrcSource::Unknown)]
    fn absent_outcomes_survive_commit_and_hint_derivation(
        #[case] state: SnapshotCrc,
        #[case] source: CrcSource,
    ) {
        let state = SnapshotCrc::try_new(state, 5, None).unwrap();
        assert!(state.at_version().is_none());
        assert!(state.base().is_none());
        let next = state.apply_commit(CrcDelta::default(), 6).only_current();
        assert_eq!(next.source(), source);
    }

    #[test]
    fn snapshot_reclassification_preserves_crc_provenance_and_sharing() {
        let crc = CrcWithProvenance::from_file(crc_at(10)).apply(CrcDelta::default(), 13);
        let original_provenance = crc.provenance().clone();
        let current = SnapshotCrc::try_new(crc.clone(), 13, Some(12)).unwrap();
        let stale = SnapshotCrc::try_new(crc, 14, Some(12)).unwrap();
        assert_eq!(
            current.source(),
            CrcSource::Reconstructed {
                on_disk_version: 10,
                crc_version: 13,
            }
        );
        assert_eq!(
            stale.source(),
            CrcSource::Stale {
                on_disk_version: 10,
                crc_version: 13,
            }
        );
        assert!(stale.at_version().is_none());
        assert!(Arc::ptr_eq(
            current.base().unwrap().crc(),
            stale.base().unwrap().crc()
        ));
        assert_eq!(current.base().unwrap().provenance(), &original_provenance);
        assert_eq!(stale.base().unwrap().provenance(), &original_provenance);
        assert!(stale.only_current().base().is_none());
        assert_eq!(stale.only_current().source(), CrcSource::Unknown);
    }
}

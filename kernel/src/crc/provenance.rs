//! Provenance of individual in-memory CRCs, independent of snapshot resolution.

use std::sync::Arc;

use super::{Crc, CrcDelta, FileStats};
use crate::Version;
#[cfg(feature = "adaptive-metadata-in-dev")]
use crate::actions::LastManifestCommit;

/// How an individual CRC was obtained. Replay retains only its original source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CrcProvenance {
    /// An observed checksum file at this version.
    File { version: Version },
    /// Connector-supplied CRC contents and any validated checksum-file ancestry.
    SnapshotHint {
        version: Version,
        on_disk_version: Option<Version>,
    },
    /// CRC state reconstructed from checkpoint actions or the full commit history.
    FullReplay { version: Version },
    /// CRC contents computed by the table's initial transaction.
    CreateTable,
    /// Committed actions applied to the original file, hint, full-replay, or CREATE TABLE CRC.
    Replayed { base: Box<CrcProvenance> },
}

impl CrcProvenance {
    fn replayed(self) -> Self {
        match self {
            already_replayed @ Self::Replayed { .. } => already_replayed,
            base => Self::Replayed {
                base: Box::new(base),
            },
        }
    }
}

/// An actual CRC with its origin, shared independently of any snapshot's version.
#[derive(Debug, Clone)]
pub(crate) struct CrcWithProvenance {
    crc: Arc<Crc>,
    provenance: CrcProvenance,
}

impl CrcWithProvenance {
    /// Records a successful checksum-file read or write.
    pub(crate) fn from_file(crc: Arc<Crc>) -> Self {
        Self {
            provenance: CrcProvenance::File {
                version: crc.version,
            },
            crc,
        }
    }

    /// Records CRC contents built by the CREATE TABLE transaction.
    pub(crate) fn from_create_table(crc: Arc<Crc>) -> Self {
        Self {
            crc,
            provenance: CrcProvenance::CreateTable,
        }
    }

    /// Records a CRC reconstructed from checkpoint actions or the full commit history.
    pub(crate) fn from_full_replay(crc: Arc<Crc>) -> Self {
        Self {
            provenance: CrcProvenance::FullReplay {
                version: crc.version,
            },
            crc,
        }
    }

    /// Records the hint's CRC version and its validated checksum-file ancestry, when supplied.
    pub(crate) fn from_snapshot_hint(crc: Arc<Crc>, on_disk_version: Option<Version>) -> Self {
        let provenance = CrcProvenance::SnapshotHint {
            version: crc.version,
            on_disk_version,
        };
        Self { crc, provenance }
    }

    /// The table version described by the payload.
    pub(crate) fn version(&self) -> Version {
        self.crc.version
    }

    /// The shared payload for APIs that operate on CRC contents.
    pub(crate) fn crc(&self) -> &Arc<Crc> {
        &self.crc
    }

    /// How the payload was obtained, independently of any snapshot's version.
    pub(crate) fn provenance(&self) -> &CrcProvenance {
        &self.provenance
    }

    /// Complete file statistics at the payload's version, when available.
    pub(crate) fn file_stats(&self) -> Option<&FileStats> {
        self.crc.file_stats()
    }

    /// The original checksum-file version, when the CRC has known file ancestry.
    pub(crate) fn on_disk_version(&self) -> Option<Version> {
        let origin = match self.provenance() {
            CrcProvenance::Replayed { base } => base.as_ref(),
            origin => origin,
        };
        match origin {
            CrcProvenance::File { version } => Some(*version),
            CrcProvenance::SnapshotHint {
                on_disk_version, ..
            } => *on_disk_version,
            _ => None,
        }
    }

    /// Replaces the payload with replay's result without adding another provenance level.
    fn with_replayed_crc(&self, crc: Arc<Crc>) -> Self {
        Self {
            crc,
            provenance: self.provenance.clone().replayed(),
        }
    }

    /// Fills commit-only fields that checkpoint reconstruction cannot provide.
    pub(crate) fn with_commit_info(
        mut self,
        in_commit_timestamp: Option<i64>,
        #[cfg(feature = "adaptive-metadata-in-dev")] last_manifest_commit: Option<
            LastManifestCommit,
        >,
    ) -> Self {
        let crc = Arc::make_mut(&mut self.crc);
        crc.in_commit_timestamp_opt = in_commit_timestamp;
        #[cfg(feature = "adaptive-metadata-in-dev")]
        {
            crc.last_manifest_commit_opt = last_manifest_commit;
        }
        self
    }

    /// Applies a committed delta and retains the original source.
    pub(crate) fn apply(&self, delta: CrcDelta, version: Version) -> Self {
        self.with_replayed_crc(Arc::new(self.crc.as_ref().clone().apply(delta, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crc_at(version: Version) -> Arc<Crc> {
        Arc::new(Crc {
            version,
            ..Default::default()
        })
    }

    #[rstest::rstest]
    #[case::file(CrcWithProvenance::from_file(crc_at(10)), CrcProvenance::File { version: 10 }, Some(10))]
    #[case::hint(CrcWithProvenance::from_snapshot_hint(crc_at(10), None), CrcProvenance::SnapshotHint { version: 10, on_disk_version: None }, None)]
    #[case::hint_with_file_ancestry(CrcWithProvenance::from_snapshot_hint(crc_at(10), Some(7)), CrcProvenance::SnapshotHint { version: 10, on_disk_version: Some(7) }, Some(7))]
    #[case::full_replay(CrcWithProvenance::from_full_replay(crc_at(10)), CrcProvenance::FullReplay { version: 10 }, None)]
    #[case::create_table(
        CrcWithProvenance::from_create_table(crc_at(0)),
        CrcProvenance::CreateTable,
        None
    )]
    fn repeated_replay_preserves_only_the_original_source(
        #[case] original: CrcWithProvenance,
        #[case] origin: CrcProvenance,
        #[case] file_version: Option<Version>,
    ) {
        let once = original.apply(CrcDelta::default(), 13);
        let twice = once.apply(CrcDelta::default(), 15);
        assert_eq!(original.provenance(), &origin);
        assert_eq!(
            once.provenance(),
            &CrcProvenance::Replayed {
                base: Box::new(origin.clone())
            }
        );
        assert_eq!(twice.provenance(), once.provenance());
        assert_eq!(twice.on_disk_version(), file_version);
        assert_eq!(twice.version(), 15);
    }

    #[rstest::rstest]
    #[case::no_claim(None)]
    #[case::at_version(Some(13))]
    #[case::reconstructed(Some(10))]
    fn hint_import_preserves_claimed_ancestry_without_copying_payload(
        #[case] origin: Option<Version>,
    ) {
        let crc = crc_at(13);
        let imported = CrcWithProvenance::from_snapshot_hint(crc.clone(), origin);
        assert!(Arc::ptr_eq(&crc, imported.crc()));
        assert_eq!(
            imported.provenance(),
            &CrcProvenance::SnapshotHint {
                version: 13,
                on_disk_version: origin,
            }
        );
        assert_eq!(imported.on_disk_version(), origin);
    }
}

//! Portable selection and exact file bytes. This manifest does not grant access.
use super::jsonl::JsonlFile;
use bigbrotr_model::{
    ArtifactId, DictionaryKind, Digest256, EventId, JobId, OwnerResourceId, QualifiedRef, SourceCut,
};
use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::io::{self, Write};

pub const MAX_MANIFEST_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestError {
    InvalidInput,
    ResourceLimit,
}

/// Inputs are retained by the execution unit. Accepted policy revisions describe
/// intent; the executor separately checks current authority before each effect.
pub struct SignedEventsManifest<'a> {
    pub artifact_id: ArtifactId,
    pub job_id: JobId,
    pub source_cut: &'a SourceCut,
    pub catalog_id: OwnerResourceId,
    pub frontier_revision: u64,
    pub snapshot_id: u64,
    pub event_id: EventId,
    pub scope: QualifiedRef,
    pub accepted_config_revision: u64,
    pub accepted_policy_revision: u64,
}
impl SignedEventsManifest<'_> {
    pub fn encode(&self, file: JsonlFile, maximum: usize) -> Result<Vec<u8>, ManifestError> {
        if maximum == 0 || self.source_cut.bounds().len() > 1024 {
            return Err(ManifestError::ResourceLimit);
        }
        if self
            .scope
            .require(
                self.source_cut.dataset_id(),
                DictionaryKind::VisibilityScope,
            )
            .is_err()
            || self.frontier_revision == 0
            || self.snapshot_id == 0
            || self.snapshot_id > i64::MAX as u64
            || self.accepted_config_revision == 0
            || self.accepted_policy_revision == 0
            || file.rows > 1
            || ((file.rows == 0) != (file.bytes == 0))
            || (file.rows == 0 && file.sha256 != Digest256::from_bytes(Sha256::digest([]).into()))
        {
            return Err(ManifestError::InvalidInput);
        }
        let document = Document {
            format: "bigbrotr-signed-events-export/1",
            artifact_id: self.artifact_id,
            job_id: self.job_id,
            visibility: "private",
            fidelity: "full-signed-events",
            origin: "retained-scoped-archive",
            source: Source {
                cut: self.source_cut,
                catalog_id: self.catalog_id,
                frontier_revision: Decimal(self.frontier_revision),
                snapshot_id: Decimal(self.snapshot_id),
            },
            selection: Selection {
                event_id: self.event_id,
                scope: self.scope,
            },
            accepted_revisions: Revisions {
                configuration: Decimal(self.accepted_config_revision),
                policy: Decimal(self.accepted_policy_revision),
            },
            included_families: ["signed-events"],
            excluded_families: [
                "internal-payload-envelopes",
                "eligibility-facts",
                "first-local-receive",
                "relay-provenance",
                "imported-claims",
                "measurements",
                "documents",
                "dictionaries",
                "coverage",
                "derived-results",
                "deployment-state",
            ],
            files: [File {
                file_id: "events",
                path: "events.jsonl",
                format: "nostr-jsonl/1",
                bytes: Decimal(file.bytes),
                rows: Decimal(file.rows),
                sha256: file.sha256,
            }],
        };
        // A fixed bounded allocation, with source cut borrowed rather than copied.
        // The execution budget must retain this buffer until manifest publication.
        let maximum = maximum.min(MAX_MANIFEST_BYTES);
        let mut output = Buffer {
            bytes: Vec::new(),
            maximum,
        };
        output
            .bytes
            .try_reserve_exact(maximum)
            .map_err(|_| ManifestError::ResourceLimit)?;
        serde_json::to_writer(&mut output, &document).map_err(|_| ManifestError::ResourceLimit)?;
        output
            .write_all(b"\n")
            .map_err(|_| ManifestError::ResourceLimit)?;
        Ok(output.bytes)
    }
}

struct Decimal(u64);
impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}
#[derive(Serialize)]
struct Document<'a> {
    format: &'static str,
    artifact_id: ArtifactId,
    job_id: JobId,
    visibility: &'static str,
    fidelity: &'static str,
    origin: &'static str,
    source: Source<'a>,
    selection: Selection,
    accepted_revisions: Revisions,
    included_families: [&'static str; 1],
    excluded_families: [&'static str; 11],
    files: [File; 1],
}
#[derive(Serialize)]
struct Source<'a> {
    cut: &'a SourceCut,
    catalog_id: OwnerResourceId,
    frontier_revision: Decimal,
    snapshot_id: Decimal,
}
#[derive(Serialize)]
struct Selection {
    event_id: EventId,
    scope: QualifiedRef,
}
#[derive(Serialize)]
struct Revisions {
    configuration: Decimal,
    policy: Decimal,
}
#[derive(Serialize)]
struct File {
    file_id: &'static str,
    path: &'static str,
    format: &'static str,
    bytes: Decimal,
    rows: Decimal,
    sha256: Digest256,
}
struct Buffer {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("export manifest byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

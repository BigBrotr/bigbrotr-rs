//! Typed requested work. A fingerprint is not acceptance or execution authority.
use super::{
    grants::{AllocationSelection, Revision},
    sessions::SessionError,
};
use bigbrotr_model::{
    CommandKey, DatasetId, DestinationId, Digest256, EventId, JobId, PrincipalId,
    read::QueryRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobCommand {
    Export(ExportCommand),
}

/// Historical acceptance only. Current job state and execution authority are
/// observed separately; replay does not restart work or refresh retained pins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobReceipt {
    pub command_key: CommandKey,
    pub job_id: JobId,
    pub accepted_at_us: i64,
    pub created: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExportFormat {
    #[serde(rename = "signed-events-jsonl")]
    SignedEventsJsonl,
}

/// Browser clients select an approved binding, never a host path or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportDestination {
    pub id: DestinationId,
    pub binding_sha256: Digest256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportCommand {
    pub format: ExportFormat,
    pub query: QueryRequest,
    pub allocation: AllocationSelection,
    pub destination: ExportDestination,
    pub expected_config_revision: Revision,
    pub expected_policy_revision: Revision,
}
impl ExportCommand {
    pub fn validate(&self, dataset: DatasetId) -> Result<EventId, SessionError> {
        if self.query.scope.dataset_id != dataset {
            return Err(SessionError::InvalidInput);
        }
        bigbrotr_protocol::read::validate(&self.query).map_err(|error| match error {
            bigbrotr_protocol::read::QueryError::Limit => SessionError::Capacity,
            _ => SessionError::InvalidInput,
        })
    }
    pub fn digest(
        &self,
        dataset: DatasetId,
        actor: PrincipalId,
        key: CommandKey,
    ) -> Result<Digest256, SessionError> {
        let event_id = self.validate(dataset)?;
        let mut hash = Sha256::new();
        hash.update(b"bigbrotr/export-event/1\0");
        for bytes in [
            dataset.as_bytes(),
            actor.as_bytes(),
            key.as_bytes(),
            self.query.scope.dataset_id.as_bytes(),
        ] {
            hash.update(bytes);
        }
        hash.update(4u16.to_be_bytes());
        hash.update(self.query.scope.allocation_domain.as_bytes());
        hash.update(self.query.scope.ordinal.get().to_be_bytes());
        hash.update(event_id.as_bytes());
        hash.update(self.query.page_size.unwrap_or(100).to_be_bytes());
        hash.update(self.allocation.catalog_id.as_bytes());
        hash.update(self.allocation.frontier_revision.get().to_be_bytes());
        hash.update(self.destination.id.as_bytes());
        hash.update(self.destination.binding_sha256.as_bytes());
        // Profile1 only: complete signed JSONL, private artifact. Neither a
        // generic export nor a private-export grant authorizes publication.
        hash.update([1, 1]);
        hash.update(self.expected_config_revision.get().to_be_bytes());
        hash.update(self.expected_policy_revision.get().to_be_bytes());
        Ok(Digest256::from_bytes(hash.finalize().into()))
    }
}
impl JobCommand {
    pub fn digest(
        &self,
        dataset: DatasetId,
        actor: PrincipalId,
        key: CommandKey,
    ) -> Result<Digest256, SessionError> {
        match self {
            Self::Export(command) => command.digest(dataset, actor, key),
        }
    }
}

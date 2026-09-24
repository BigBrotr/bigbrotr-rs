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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JobCommand {
    Export(ExportCommand),
}

impl<'de> Deserialize<'de> for JobCommand {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::{Error, MapAccess, Visitor};
        // Serde's derived internally tagged enum buffers every value in a
        // generic Content tree before deny_unknown_fields runs. Nested unknown
        // arrays exceeded the HTTP reservation with only6KiB of input. Decode
        // this closed map directly and reject unknown/duplicate keys before
        // reading their values; kind remains independent of field order.
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Kind,
            Format,
            Query,
            Allocation,
            Destination,
            ExpectedConfigRevision,
            ExpectedPolicyRevision,
        }
        #[derive(Deserialize)]
        enum Kind {
            #[serde(rename = "export")]
            Export,
        }
        fn field<'de, M: MapAccess<'de>, T: Deserialize<'de>>(
            map: &mut M,
            slot: &mut Option<T>,
            name: &'static str,
        ) -> Result<(), M::Error> {
            if slot.is_some() {
                return Err(M::Error::duplicate_field(name));
            }
            *slot = Some(map.next_value()?);
            Ok(())
        }
        struct CommandVisitor;
        impl<'de> Visitor<'de> for CommandVisitor {
            type Value = JobCommand;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a closed typed job command object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<JobCommand, M::Error> {
                let (
                    mut kind,
                    mut format,
                    mut query,
                    mut allocation,
                    mut destination,
                    mut expected_config_revision,
                    mut expected_policy_revision,
                ) = (None::<Kind>, None, None, None, None, None, None);
                while let Some(key) = map.next_key::<Field>()? {
                    match key {
                        Field::Kind => field(&mut map, &mut kind, "kind")?,
                        Field::Format => field(&mut map, &mut format, "format")?,
                        Field::Query => field(&mut map, &mut query, "query")?,
                        Field::Allocation => field(&mut map, &mut allocation, "allocation")?,
                        Field::Destination => field(&mut map, &mut destination, "destination")?,
                        Field::ExpectedConfigRevision => field(
                            &mut map,
                            &mut expected_config_revision,
                            "expected_config_revision",
                        )?,
                        Field::ExpectedPolicyRevision => field(
                            &mut map,
                            &mut expected_policy_revision,
                            "expected_policy_revision",
                        )?,
                    }
                }
                kind.ok_or_else(|| M::Error::missing_field("kind"))?;
                Ok(JobCommand::Export(ExportCommand {
                    format: format.ok_or_else(|| M::Error::missing_field("format"))?,
                    query: query.ok_or_else(|| M::Error::missing_field("query"))?,
                    allocation: allocation.ok_or_else(|| M::Error::missing_field("allocation"))?,
                    destination: destination
                        .ok_or_else(|| M::Error::missing_field("destination"))?,
                    expected_config_revision: expected_config_revision
                        .ok_or_else(|| M::Error::missing_field("expected_config_revision"))?,
                    expected_policy_revision: expected_policy_revision
                        .ok_or_else(|| M::Error::missing_field("expected_policy_revision"))?,
                }))
            }
        }
        deserializer.deserialize_map(CommandVisitor)
    }
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

/// Redacted operational metadata of the authenticated actor's own export.
/// This neither refreshes input grants nor authorizes artifact access or writes.
pub struct JobObservation {
    pub job_id: JobId,
    pub state: crate::export::execution::ExportState,
    pub job_revision: u64,
    pub checkpoint_revision: u64,
    pub created_at_us: i64,
    pub completed_at_us: Option<i64>,
    pub observed_at_us: i64,
    pub reason: Option<JobReason>,
}

/// Never forward arbitrary database diagnostic text to a browser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobReason {
    ResourceLimit,
    ExecutionRefused,
    Other,
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

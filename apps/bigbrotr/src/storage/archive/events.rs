//! Private historical hydration. Caller retains read authority and source pins.
use super::{CatalogReaderCredentials, CatalogRoute, Preparation, PreparationError, ProcessError};
use crate::runtime::{OwnedPermit, ResourceBudget, ResourceRequest};
use bigbrotr_protocol::{
    VerifiedEvent,
    archive_event::{
        EventObservation, EventQuery, EventQueryObservation, EventRead, EventReadError,
        EventSelection, QUERY_FORMAT,
    },
};
use serde::Serialize;
use std::{sync::atomic::Ordering, time::Instant};

/// The grant covers the decoded body until its last consumer drops this value.
/// Neither a caller-declared scope nor this read provides an access capability.
pub struct ArchivedEvent {
    event: VerifiedEvent,
    snapshot_id: u64,
    _retention: Retention,
}
struct Retention {
    _permit: OwnedPermit,
    // A decoded body can outlive its native job and originating coordinator.
    // Delivery must retain local exclusion as well as its allocation grant.
    _custody: super::ReaderCustody,
}
impl ArchivedEvent {
    pub fn event(&self) -> &VerifiedEvent {
        &self.event
    }
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }
}
/// A completed exact-selection lookup. Absence retains the same allocation and
/// catalog custody as presence until the final consumer finishes delivery.
pub struct ArchivedEventQuery {
    event: Option<VerifiedEvent>,
    query: EventQuery,
    _retention: Retention,
}
impl ArchivedEventQuery {
    pub fn event(&self) -> Option<&VerifiedEvent> {
        self.event.as_ref()
    }
    pub fn query(&self) -> &EventQuery {
        &self.query
    }
}
#[derive(Debug)]
pub enum ArchiveEventError {
    Input(EventReadError),
    Reader(super::ReceiptError),
    ResourceLimit,
    Preparation(PreparationError),
    ObservationUnknown,
    TerminationUnknown,
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: Selection<'a>,
}
#[derive(Clone, Copy, Serialize)]
#[serde(untagged)]
enum Selection<'a> {
    References(&'a EventRead),
    Query(&'a EventQuery),
}
impl Preparation<'_> {
    /// Read two exact immutable references in one fresh native transaction.
    /// The caller MUST keep policy/retention authority live through delivery;
    /// unlike receipt diagnostics, event data is not a post-cleanup result.
    pub fn read_event(
        self,
        expected: &EventRead,
        coordinator: &super::LocalCoordinator,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
    ) -> Result<ArchivedEvent, ArchiveEventError> {
        let custody = coordinator
            .reader_custody()
            .map_err(|e| ArchiveEventError::Preparation(PreparationError::Custody(e)))?;
        let (event, snapshot_id, retention) = self.read_selected(
            Selection::References(expected),
            custody,
            route,
            credential,
            budget,
        )?;
        Ok(ArchivedEvent {
            event: event.ok_or(ArchiveEventError::ObservationUnknown)?,
            snapshot_id,
            _retention: retention,
        })
    }

    /// Look up an ID under one permitted scope, exact native snapshot and source
    /// cut. The caller must bind these inputs to its live authority and pin.
    pub fn query_event(
        self,
        expected: EventQuery,
        custody: super::ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
    ) -> Result<ArchivedEventQuery, ArchiveEventError> {
        let (event, _, retention) = self.read_selected(
            Selection::Query(&expected),
            custody,
            route,
            credential,
            budget,
        )?;
        Ok(ArchivedEventQuery {
            event,
            query: expected,
            _retention: retention,
        })
    }

    fn read_selected(
        self,
        expected: Selection<'_>,
        custody: super::ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
    ) -> Result<(Option<VerifiedEvent>, u64, Retention), ArchiveEventError> {
        let maximum = self.limits.max_encoded_bytes;
        let identity = custody.identity();
        let (dataset, command) = match expected {
            Selection::References(read) => {
                read.validate(maximum).map_err(ArchiveEventError::Input)?;
                (read.dataset, "read-event")
            }
            Selection::Query(query) => {
                query.validate(maximum).map_err(ArchiveEventError::Input)?;
                (query.dataset, "query-event")
            }
        };
        let output_bytes = maximum + 2048;
        // Conservative allocation floor for two native rows, JSON/Arrow UTF-8
        // conversion, nested empty tags, reconstructed facts and signature work.
        // Engine/native overhead still needs its separate reservation and OS cap.
        let overlap = 64 * output_bytes as u64;
        if self.limits.native_reserve_bytes < 16 * 1024 * 1024 + overlap {
            return Err(ArchiveEventError::ResourceLimit);
        }
        let (dsn, tls) = super::receipts::reader_connection(
            dataset,
            identity,
            route,
            credential,
            false,
            "bigbrotr-archive-event",
        )
        .map_err(ArchiveEventError::Reader)?;
        let input = Input {
            catalog: super::bootstrap::Input {
                dsn: &dsn,
                data_root: identity.root(),
            },
            catalog_id: identity.catalog(),
            expected,
        };
        input
            .catalog
            .validate()
            .map_err(ArchiveEventError::Preparation)?;
        super::bootstrap::wire_fits(&input).map_err(ArchiveEventError::Preparation)?;
        let grant = budget
            .try_acquire(
                ResourceRequest::data(overlap, 1, 0)
                    .map_err(|_| ArchiveEventError::ResourceLimit)?,
            )
            .map_err(|_| ArchiveEventError::ResourceLimit)?;
        let mut process = self
            .start_reader(command, &[maximum.to_string()], budget, &custody, tls)
            .map_err(ArchiveEventError::Preparation)?;
        if process.reserve_data_output(output_bytes, grant).is_err() {
            // Allocation failure happens after spawn. Observe termination before
            // reporting a retryable resource refusal; Drop alone cannot expose
            // an unknown live child to the caller.
            if matches!(process.finish(), Err(ProcessError::TerminationUnknown)) {
                return Err(ArchiveEventError::TerminationUnknown);
            }
            return Err(ArchiveEventError::ResourceLimit);
        }
        let transfer = serde_json::to_writer(&mut process, &input);
        let result = process.take_data_output();
        if matches!(result, Err(ProcessError::TerminationUnknown)) {
            return Err(ArchiveEventError::TerminationUnknown);
        }
        if transfer.is_err() {
            return Err(ArchiveEventError::ObservationUnknown);
        }
        let (bytes, permit) = result.map_err(|_| ArchiveEventError::ObservationUnknown)?;
        let (event, snapshot_id) = match expected {
            Selection::References(read) => {
                let (event, snapshot) = decode_response(&bytes, read, maximum)?;
                (Some(event), snapshot)
            }
            Selection::Query(query) => decode_match(&bytes, query, maximum)?,
        };
        if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            return Err(ArchiveEventError::ObservationUnknown);
        }
        Ok((
            event,
            snapshot_id,
            Retention {
                _permit: permit,
                _custody: custody,
            },
        ))
    }
}

fn decode_match(
    bytes: &[u8],
    expected: &EventQuery,
    maximum: usize,
) -> Result<(Option<VerifiedEvent>, u64), ArchiveEventError> {
    let response: EventQueryObservation =
        serde_json::from_slice(bytes).map_err(|_| ArchiveEventError::ObservationUnknown)?;
    if response.format != QUERY_FORMAT
        || response.snapshot_id != expected.snapshot_id
        || response.request_digest != expected.digest()
    {
        return Err(ArchiveEventError::ObservationUnknown);
    }
    let observation = match response.result {
        EventSelection::Absent {} => return Ok((None, response.snapshot_id)),
        EventSelection::Present { matched } => matched,
    };
    if observation.event.id != expected.event_id
        || observation.snapshot_id != expected.snapshot_id
        || !expected.contains(observation.payload.source_record_id)
        || !expected.contains(observation.eligibility.source_record_id)
        || observation.payload.source_record_id == observation.eligibility.source_record_id
    {
        return Err(ArchiveEventError::ObservationUnknown);
    }
    let event = bigbrotr_protocol::verify_owned(observation.event, maximum)
        .map_err(|_| ArchiveEventError::ObservationUnknown)?;
    Ok((Some(event), observation.snapshot_id))
}

fn decode_response(
    bytes: &[u8],
    expected: &EventRead,
    maximum: usize,
) -> Result<(VerifiedEvent, u64), ArchiveEventError> {
    let observation: EventObservation =
        serde_json::from_slice(bytes).map_err(|_| ArchiveEventError::ObservationUnknown)?;
    if observation.event.id != expected.event_id
        || expected
            .snapshot_id
            .is_some_and(|id| id != observation.snapshot_id)
    {
        return Err(ArchiveEventError::ObservationUnknown);
    }
    let event = bigbrotr_protocol::verify_owned(observation.event, maximum)
        .map_err(|_| ArchiveEventError::ObservationUnknown)?;
    Ok((event, observation.snapshot_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completed_empty_query_is_distinct_from_missing_or_failed_output() {
        let dataset = [1; 16].try_into().unwrap();
        let query = EventQuery {
            dataset,
            event_id: bigbrotr_model::EventId::from_bytes([2; 32]),
            scope: bigbrotr_model::QualifiedRef {
                dataset_id: dataset,
                dictionary_kind: bigbrotr_model::DictionaryKind::VisibilityScope,
                allocation_domain: [3; 16].try_into().unwrap(),
                ordinal: bigbrotr_model::Ordinal::new(1).unwrap(),
            },
            topology_generation: bigbrotr_model::TopologyGeneration::new(0),
            bounds: vec![bigbrotr_protocol::archive_cut::CutBound {
                shard_id: 0,
                owner_epoch: 1,
                complete_through: 0,
            }],
            snapshot_id: 2,
        };
        let bytes =
            include_bytes!("../../../../../tests/fixtures/protocol/archive-event-absence.json");
        let actual = decode_match(bytes, &query, 4096);
        assert!(actual.is_ok(), "completed absence must decode: {actual:?}");
        assert!(actual.unwrap().0.is_none());
        let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        for field in ["format", "snapshot_id", "request_digest", "result"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(decode_match(&serde_json::to_vec(&missing).unwrap(), &query, 4096).is_err());
        }
        for field in ["format", "snapshot_id", "request_digest", "unknown"] {
            let mut bad = value.clone();
            bad[field] = if field == "snapshot_id" {
                serde_json::json!(3)
            } else {
                serde_json::json!("bad")
            };
            assert!(decode_match(&serde_json::to_vec(&bad).unwrap(), &query, 4096).is_err());
        }
        for field in [
            "dataset",
            "event",
            "scope-dataset",
            "kind",
            "domain",
            "ordinal",
            "topology",
            "snapshot",
            "shard",
            "epoch",
            "prefix",
            "count",
        ] {
            let mut other = query.clone();
            match field {
                "dataset" => other.dataset = [4; 16].try_into().unwrap(),
                "event" => other.event_id = bigbrotr_model::EventId::from_bytes([4; 32]),
                "scope-dataset" => other.scope.dataset_id = [4; 16].try_into().unwrap(),
                "kind" => other.scope.dictionary_kind = bigbrotr_model::DictionaryKind::Event,
                "domain" => other.scope.allocation_domain = [4; 16].try_into().unwrap(),
                "ordinal" => other.scope.ordinal = bigbrotr_model::Ordinal::new(2).unwrap(),
                "topology" => {
                    other.topology_generation = bigbrotr_model::TopologyGeneration::new(1)
                }
                "snapshot" => other.snapshot_id += 1,
                "shard" => other.bounds[0].shard_id += 1,
                "epoch" => other.bounds[0].owner_epoch += 1,
                "prefix" => other.bounds[0].complete_through += 1,
                _ => other.bounds.clear(),
            }
            assert!(decode_match(bytes, &other, 4096).is_err(), "{field}");
        }
        for invalid in [
            serde_json::json!({"status":"absent","matched":{}}),
            serde_json::json!({"status":"present"}),
            serde_json::json!({"status":"missing"}),
        ] {
            let mut bad = value.clone();
            bad["result"] = invalid;
            assert!(decode_match(&serde_json::to_vec(&bad).unwrap(), &query, 4096).is_err());
        }
        for bytes in [b"".as_slice(), b"{}", b"null", b"{\"status\":\"absent\"}"] {
            assert!(decode_match(bytes, &query, 4096).is_err());
        }
    }
    #[test]
    fn complete_signed_response_requires_the_requested_snapshot() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/protocol/signed-extremes.json"
        ))
        .unwrap();
        let event = fixtures["events"]["timestamp-zero"]["event"].clone();
        let reference = |sequence| serde_json::json!({"source_record_id":{"dataset_id":"01".repeat(16),"shard_id":0,"owner_epoch":"1","sequence":sequence},"payload_digest":"02".repeat(32)});
        let mut request: EventRead = serde_json::from_value(serde_json::json!({
            "dataset":"01".repeat(16),"event_id":event["id"],"scope":{"dataset_id":"01".repeat(16),"dictionary_kind":4,"allocation_domain":"03".repeat(16),"ordinal":"1"},"payload":reference("1"),"eligibility":reference("2"),"snapshot_id":2
        })).unwrap();
        let bytes =
            serde_json::to_vec(&serde_json::json!({"snapshot_id":2,"event":event})).unwrap();
        let (actual, snapshot) = decode_response(&bytes, &request, 4096).unwrap();
        assert_eq!(serde_json::to_value(actual.event()).unwrap(), event);
        assert_eq!(snapshot, 2);
        request.snapshot_id = Some(3);
        assert!(matches!(
            decode_response(&bytes, &request, 4096),
            Err(ArchiveEventError::ObservationUnknown)
        ));
        request.snapshot_id = None;
        assert!(decode_response(&bytes, &request, 4096).is_ok());

        let query = EventQuery {
            dataset: request.dataset,
            event_id: request.event_id,
            scope: request.scope,
            topology_generation: bigbrotr_model::TopologyGeneration::new(0),
            bounds: vec![bigbrotr_protocol::archive_cut::CutBound {
                shard_id: 0,
                owner_epoch: 1,
                complete_through: 2,
            }],
            snapshot_id: 2,
        };
        let selected = serde_json::json!({"format":QUERY_FORMAT,"snapshot_id":2,"request_digest":query.digest(),"result":{"status":"present","matched":{"snapshot_id":2,"event":event,"payload":request.payload,"eligibility":request.eligibility}}});
        assert!(decode_match(&serde_json::to_vec(&selected).unwrap(), &query, 4096).is_ok());
        for field in ["payload", "eligibility"] {
            for (part, value) in [
                ("sequence", "3"),
                ("owner_epoch", "2"),
                ("dataset_id", &"04".repeat(16)),
            ] {
                let mut outside = selected.clone();
                outside["result"]["matched"][field]["source_record_id"][part] =
                    serde_json::json!(value);
                assert!(
                    matches!(
                        decode_match(&serde_json::to_vec(&outside).unwrap(), &query, 4096),
                        Err(ArchiveEventError::ObservationUnknown)
                    ),
                    "{field}/{part}"
                );
            }
        }
        let mut duplicate = selected.clone();
        duplicate["result"]["matched"]["eligibility"] =
            duplicate["result"]["matched"]["payload"].clone();
        assert!(decode_match(&serde_json::to_vec(&duplicate).unwrap(), &query, 4096).is_err());
        let mut wrong_snapshot = query.clone();
        wrong_snapshot.snapshot_id = 3;
        assert!(
            decode_match(
                &serde_json::to_vec(&selected).unwrap(),
                &wrong_snapshot,
                4096
            )
            .is_err()
        );
    }
}

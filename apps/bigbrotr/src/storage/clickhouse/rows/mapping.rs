use super::{
    DictionaryRow, EligibilityRow, Envelope, EventIndexRow, EventRow, IndexTarget, ProvenanceRow,
    ReceiveRow, RowsError, TagRow, reference_tuple, source_tuple,
};
use bigbrotr_model::{
    DictionaryDescriptor, Fact, FactPayload, ProjectionGenerationId, codec::FactDecodeLimits,
    dictionary::EVENT_VERIFICATION_NIPS_REVISION,
};
use std::{borrow::Cow, mem::size_of};

/// No source payloads are copied here. The caller retains the decoded fact and
/// its reservation until all borrowed rows finish serialization. Tag storage is
/// bounded by the codec's actual per-fact node limit, including duplicates.
pub struct FactRows<'a> {
    fact: &'a Fact,
    envelope: Envelope,
    tags: Vec<(&'a str, &'a str)>,
    body_digest: Option<[u8; 32]>,
}

macro_rules! mapped_row {
    ($name:ident, $envelope:expr, $discriminator:expr, {$($field:ident: $value:expr),* $(,)?}) => {{
        let envelope = $envelope;
        $name {
            projection_generation: envelope.projection_generation,
            source: envelope.source,
            record_schema_version: envelope.record_schema_version,
            visibility_scope: envelope.visibility_scope,
            recorded_at: envelope.recorded_at,
            payload_digest: envelope.payload_digest,
            discriminator: $discriminator,
            $($field: $value,)*
        }
    }};
}

fn posting(tag: &[String]) -> Option<(&str, &str)> {
    let [key, value, ..] = tag else { return None };
    (key.len() == 1 && key.as_bytes()[0].is_ascii_alphabetic())
        .then_some((key.as_str(), value.as_str()))
}

impl<'a> FactRows<'a> {
    pub fn new(
        generation: ProjectionGenerationId,
        fact: &'a Fact,
        provenance_enabled: bool,
        limits: FactDecodeLimits,
    ) -> Result<Self, RowsError> {
        // Off is a data invariant, not permission to silently discard retained
        // witness facts while advancing their source frontier.
        if !provenance_enabled && matches!(fact.payload(), FactPayload::Provenance(_)) {
            return Err(RowsError::ProvenanceDisabled);
        }
        let mut tags = Vec::new();
        if let FactPayload::EventPayload(event) = fact.payload() {
            let nodes = event
                .body
                .tags
                .iter()
                .try_fold(event.body.tags.len(), |n, tag| n.checked_add(tag.len()))
                .ok_or(RowsError::SizeOverflow)?;
            let text = event
                .body
                .tags
                .iter()
                .flatten()
                .try_fold(event.body.content.len(), |n, text| {
                    n.checked_add(text.len())
                })
                .ok_or(RowsError::SizeOverflow)?;
            if nodes > super::MAX_CORE_FACT_BYTES || text > super::MAX_CORE_FACT_BYTES {
                return Err(RowsError::IncompatibleProfile);
            }
            let count = event
                .body
                .tags
                .iter()
                .filter_map(|tag| posting(tag))
                .count();
            let bytes = count
                .checked_mul(size_of::<(&str, &str)>())
                .ok_or(RowsError::SizeOverflow)?;
            if count > limits.max_tag_nodes || bytes > limits.max_allocation_bytes {
                return Err(RowsError::TagLimit);
            }
            tags.try_reserve_exact(count)
                .map_err(|_| RowsError::Allocation)?;
            tags.extend(event.body.tags.iter().filter_map(|tag| posting(tag)));
            // Tuple/str comparison is lexicographic over unchanged UTF-8 bytes.
            // Signed body tags retain their original ordering and duplicates.
            tags.sort_unstable();
            tags.dedup();
        }
        Ok(Self {
            fact,
            envelope: Envelope::new(generation, fact),
            tags,
            body_digest: match fact.payload() {
                FactPayload::EventPayload(event) => Some(*event.body.body_digest().as_bytes()),
                _ => None,
            },
        })
    }

    /// Maximum additional tag-reference allocation to reserve before mapping a
    /// fact under these decoder limits. The source decoder/driver retain their
    /// separate grants. Requested allocation is not an RSS promise.
    pub fn required_tag_reference_bytes(limits: FactDecodeLimits) -> Result<usize, RowsError> {
        limits
            .max_tag_nodes
            .min(super::MAX_CORE_FACT_BYTES)
            .checked_mul(size_of::<(&str, &str)>())
            .map(|bytes| bytes.min(limits.max_allocation_bytes))
            .ok_or(RowsError::SizeOverflow)
    }

    pub fn tag_reference_bytes(&self) -> usize {
        self.tags.capacity() * size_of::<(&str, &str)>()
    }

    /// Explicit outcomes for all ten initial targets, in IndexTarget::ALL order.
    /// Zero-output targets still require a complete chunk digest and publication
    /// outcome; an absent target is never inferred from a missing insertion.
    pub fn counts(&self) -> [u64; 10] {
        match self.fact.payload() {
            FactPayload::Dictionary(_) => [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            FactPayload::EventPayload(_) => [0, 1, 0, 0, 1, 1, 1, self.tags.len() as u64, 0, 0],
            FactPayload::EventEligibility(_) => [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
            FactPayload::EventReceive(_) => [0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
            FactPayload::Provenance(_) => [0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
        }
    }

    pub fn dictionary(&self) -> Option<DictionaryRow<'a>> {
        let FactPayload::Dictionary(entry) = self.fact.payload() else {
            return None;
        };
        let mut row = mapped_row!(DictionaryRow, self.envelope, 0, {
            dictionary_ref: reference_tuple(entry.reference()),
            normalization_version: entry.normalization_version(),
            descriptor_schema: Cow::Borrowed(entry.descriptor_schema()),
            descriptor_version: entry.descriptor_version(),
            descriptor_digest: *entry.digest().as_bytes(),
            event_id: None, class: None, url: None, clock: None, vantage: None,
            software: None, method_id: None, public_key: None, route_class: None,
            software_version: None, label: None, access_visibility_scope: None,
            input: None, algorithm: None, capability: None, output_unit: None,
            failure_taxonomy: None, algorithm_version: None, protocol_revision: None,
            format: None, format_version: None, source_dataset_id: None,
            source_artifact_digest: None,
        });
        match entry.descriptor() {
            DictionaryDescriptor::Event(id) => row.event_id = Some(*id.as_bytes()),
            DictionaryDescriptor::Visibility { class } => {
                row.class = Some(Cow::Borrowed(class.as_str()))
            }
            DictionaryDescriptor::Endpoint(value) => row.url = Some(Cow::Borrowed(value.url())),
            DictionaryDescriptor::Observer(value) => {
                row.clock = value.clock.map(|v| Cow::Borrowed(v.as_str()));
                row.vantage = value.vantage.as_ref().map(|v| Cow::Borrowed(v.as_str()));
                row.software = value.software.as_ref().map(|v| Cow::Borrowed(v.as_str()));
                row.method_id = value.method_id.map(reference_tuple);
                row.public_key = value.public_key.map(|v| *v.as_bytes());
                row.route_class = value.route_class.map(|v| Cow::Borrowed(v.as_str()));
                row.software_version = value
                    .software_version
                    .as_ref()
                    .map(|v| Cow::Borrowed(v.as_str()));
            }
            DictionaryDescriptor::AccessScope(value) => {
                row.class = Some(Cow::Borrowed(value.class.as_str()));
                row.label = value.label.as_ref().map(|v| Cow::Borrowed(v.as_str()));
                row.access_visibility_scope = Some(reference_tuple(value.visibility_scope_id));
            }
            DictionaryDescriptor::EventVerification => {
                row.input = Some(Cow::Borrowed("nip01-event/1"));
                row.algorithm = Some(Cow::Borrowed("sha256-bip340"));
                row.capability = Some(Cow::Borrowed("event-id-and-signature"));
                row.output_unit = Some(Cow::Borrowed("verification-result"));
                row.failure_taxonomy = Some(Cow::Borrowed("bigbrotr-nip01-verifier/1"));
                row.algorithm_version = Some(1);
                row.protocol_revision = Some(Cow::Borrowed(EVENT_VERIFICATION_NIPS_REVISION));
            }
            DictionaryDescriptor::DatasetLineage(value) => {
                row.format = Some(Cow::Borrowed(value.format.as_str()));
                row.format_version = Some(value.format_version);
                row.source_dataset_id = value.source_dataset_id.map(|v| *v.as_bytes());
                row.source_artifact_digest = Some(*value.source_artifact_digest.as_bytes());
            }
        }
        Some(row)
    }

    pub fn events(&self) -> Option<EventRow<'a>> {
        let FactPayload::EventPayload(event) = self.fact.payload() else {
            return None;
        };
        let body = &event.body;
        Some(mapped_row!(EventRow, self.envelope, 0, {
            event_id: *body.event_id.as_bytes(), pubkey: *body.pubkey.as_bytes(),
            created_at: body.created_at.0, kind: body.kind,
            tags: Cow::Borrowed(&body.tags), content: Cow::Borrowed(&body.content),
            event_ordinal: reference_tuple(event.event_ordinal),
            body_digest: self.body_digest.expect("event payload has a body digest"),
        }))
    }

    pub fn event_eligibility_facts(&self) -> Option<EligibilityRow<'a>> {
        let FactPayload::EventEligibility(value) = self.fact.payload() else {
            return None;
        };
        Some(mapped_row!(EligibilityRow, self.envelope, 0, {
            event_id: *value.event_id.as_bytes(), body_digest: *value.body_digest.as_bytes(),
            sig: *value.sig.as_bytes(), first_eligible_at: value.first_eligible_at.get(),
            origin: Cow::Borrowed(value.origin.as_str()),
            import_lineage: value.origin.import_lineage().map(reference_tuple),
        }))
    }

    pub fn event_receive_facts(&self) -> Option<ReceiveRow> {
        let FactPayload::EventReceive(value) = self.fact.payload() else {
            return None;
        };
        let clock = value.clock_quality;
        Some(mapped_row!(ReceiveRow, self.envelope, 0, {
            event_id: *value.event_id.as_bytes(), received_at: value.received_at.get(),
            observer_id: reference_tuple(value.observer_id), clock_id: reference_tuple(clock.clock_id()),
            resolution_us: clock.resolution_us().get(), uncertainty_us: clock.uncertainty_us().map(|v| v.get()),
            anomaly_flags: clock.anomaly_flags(), corrects: value.corrects.map(source_tuple),
        }))
    }

    pub fn event_index(&self) -> Option<EventIndexRow> {
        let FactPayload::EventPayload(event) = self.fact.payload() else {
            return None;
        };
        let body = &event.body;
        Some(mapped_row!(EventIndexRow, self.envelope, 0, {
            event_id: *body.event_id.as_bytes(), pubkey: *body.pubkey.as_bytes(),
            created_at: body.created_at.0, kind: body.kind,
            body_digest: self.body_digest.expect("event payload has a body digest"),
        }))
    }

    pub fn event_tag_index(&self) -> impl ExactSizeIterator<Item = TagRow<'a>> + '_ {
        let event_id = self.fact.payload().event_id().map(|v| *v.as_bytes());
        self.tags
            .iter()
            .enumerate()
            .map(move |(ordinal, &(key, value))| {
                mapped_row!(TagRow, self.envelope, ordinal as u64, {
                    event_id: event_id.expect("only event payloads have tag postings"),
                    tag_key: Cow::Borrowed(key), tag_value: Cow::Borrowed(value),
                })
            })
    }

    pub fn provenance(&self) -> Option<ProvenanceRow> {
        let FactPayload::Provenance(value) = self.fact.payload() else {
            return None;
        };
        let clock = value.clock_quality;
        Some(mapped_row!(ProvenanceRow, self.envelope, 0, {
            event_id: *value.event_id.as_bytes(), endpoint_id: reference_tuple(value.endpoint_id),
            observer_id: reference_tuple(value.observer_id), access_scope_id: reference_tuple(value.access_scope_id),
            received_at: value.received_at.get(), clock_id: reference_tuple(clock.clock_id()),
            resolution_us: clock.resolution_us().get(), uncertainty_us: clock.uncertainty_us().map(|v| v.get()),
            anomaly_flags: clock.anomaly_flags(), verification_method_id: reference_tuple(value.verification_method_id),
            corrects: value.corrects.map(source_tuple),
        }))
    }

    /// Closed dispatch for planning: each target has one accumulator, including
    /// zero-row outcomes. Inserts/readback use the same public typed getters.
    pub fn hash_into(&self, chunks: &mut [super::ChunkHasher; 10]) -> Result<(), RowsError> {
        for (target, chunk) in IndexTarget::ALL.into_iter().zip(chunks) {
            if chunk.target() != target {
                return Err(RowsError::WrongTarget);
            }
            match target {
                IndexTarget::Dictionaries => {
                    if let Some(row) = self.dictionary() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::Events => {
                    if let Some(row) = self.events() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::EventEligibilityFacts => {
                    if let Some(row) = self.event_eligibility_facts() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::EventReceiveFacts => {
                    if let Some(row) = self.event_receive_facts() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::EventIdIndex
                | IndexTarget::EventAuthorIndex
                | IndexTarget::EventTimeIndex => {
                    if let Some(row) = self.event_index() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::EventTagIndex => {
                    for row in self.event_tag_index() {
                        chunk.push(&row)?;
                    }
                }
                IndexTarget::ProvenanceByEvent | IndexTarget::ProvenanceByRelay => {
                    if let Some(row) = self.provenance() {
                        chunk.push(&row)?;
                    }
                }
            }
        }
        Ok(())
    }
}

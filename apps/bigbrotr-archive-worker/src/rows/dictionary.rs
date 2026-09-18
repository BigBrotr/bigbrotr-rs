use super::*;
use arrow::array::StringArray;
use bigbrotr_model::{DictionaryDescriptor, DictionaryEntry};

impl Rows {
    pub fn dictionaries(facts: &[&Fact], limits: RowLimits) -> Result<Self, RowError> {
        validate_rows(facts, limits)?;
        let mut text_left = limits.max_text_bytes;
        for fact in facts {
            for text in Descriptor::of(entry(fact)?.descriptor())
                .texts()
                .into_iter()
                .flatten()
            {
                text_left = text_left
                    .checked_sub(text.len())
                    .ok_or(RowError::TextLimit)?;
            }
        }
        let mut entries = Vec::new();
        let mut values = Vec::new();
        entries
            .try_reserve_exact(facts.len())
            .map_err(|_| RowError::Allocation)?;
        values
            .try_reserve_exact(facts.len())
            .map_err(|_| RowError::Allocation)?;
        for fact in facts {
            let entry = entry(fact)?;
            entries.push(entry);
            values.push(Descriptor::of(entry.descriptor()));
        }
        // Closed variant fields: schema identifies which fields apply. Nullable
        // typed values preserve unknown attribution and inactive variants; no
        // arbitrary JSON or extra descriptor fields can enter this structure.
        let descriptor = vec![
            (
                "event_id",
                optional_fixed(values.iter().map(|v| v.event_id))?,
            ),
            ("class", strings(values.iter().map(|v| v.class))),
            ("url", strings(values.iter().map(|v| v.url))),
            ("clock", strings(values.iter().map(|v| v.clock))),
            ("vantage", strings(values.iter().map(|v| v.vantage))),
            ("software", strings(values.iter().map(|v| v.software))),
            (
                "method_id",
                optional_qualified(values.iter().map(|v| v.method_id))?,
            ),
            (
                "public_key",
                optional_fixed(values.iter().map(|v| v.public_key))?,
            ),
            ("route_class", strings(values.iter().map(|v| v.route_class))),
            (
                "software_version",
                strings(values.iter().map(|v| v.software_version)),
            ),
            ("label", strings(values.iter().map(|v| v.label))),
            (
                "visibility_scope_id",
                optional_qualified(values.iter().map(|v| v.visibility_scope_id))?,
            ),
            ("input", strings(values.iter().map(|v| v.input))),
            ("algorithm", strings(values.iter().map(|v| v.algorithm))),
            ("capability", strings(values.iter().map(|v| v.capability))),
            ("output_unit", strings(values.iter().map(|v| v.output_unit))),
            (
                "failure_taxonomy",
                strings(values.iter().map(|v| v.failure_taxonomy)),
            ),
            (
                "algorithm_version",
                Arc::new(UInt16Array::from_iter(
                    values.iter().map(|v| v.algorithm_version),
                )),
            ),
            (
                "protocol_revision",
                strings(values.iter().map(|v| v.protocol_revision)),
            ),
            ("format", strings(values.iter().map(|v| v.format))),
            (
                "format_version",
                Arc::new(UInt16Array::from_iter(
                    values.iter().map(|v| v.format_version),
                )),
            ),
            (
                "source_dataset_id",
                optional_fixed(values.iter().map(|v| v.source_dataset_id))?,
            ),
            (
                "source_artifact_digest",
                optional_fixed(values.iter().map(|v| v.source_artifact_digest))?,
            ),
        ];
        let nullable: Vec<_> = descriptor.iter().map(|(name, _)| *name).collect();
        let descriptor = nullable_structure(descriptor, &nullable, None)?;
        let mut columns = envelope(facts)?;
        columns.extend([
            (
                "reference",
                qualified(entries.iter().map(|v| v.reference()))?,
            ),
            (
                "normalization_version",
                Arc::new(UInt16Array::from_iter_values(
                    entries.iter().map(|v| v.normalization_version()),
                )) as ArrayRef,
            ),
            (
                "descriptor_schema",
                Arc::new(StringArray::from_iter_values(
                    entries.iter().map(|v| v.descriptor_schema()),
                )),
            ),
            (
                "descriptor_version",
                Arc::new(UInt16Array::from_iter_values(
                    entries.iter().map(|v| v.descriptor_version()),
                )),
            ),
            ("descriptor_value", descriptor),
        ]);
        Self::finish(columns, &[])
    }
}

fn entry(fact: &Fact) -> Result<&DictionaryEntry, RowError> {
    match fact.payload() {
        FactPayload::Dictionary(value) => Ok(value),
        _ => Err(RowError::WrongFamily),
    }
}
fn strings<'a>(values: impl ExactSizeIterator<Item = Option<&'a str>> + Clone) -> ArrayRef {
    let bytes = values.clone().flatten().map(str::len).sum();
    let mut builder = StringBuilder::with_capacity(values.len(), bytes);
    for value in values {
        builder.append_option(value);
    }
    Arc::new(builder.finish())
}
fn optional_fixed<'a, const N: usize>(
    values: impl ExactSizeIterator<Item = Option<&'a [u8; N]>>,
) -> Result<ArrayRef, RowError> {
    fixed_options(values.map(|v| v.copied()))
}

#[derive(Default)]
struct Descriptor<'a> {
    event_id: Option<&'a [u8; 32]>,
    class: Option<&'a str>,
    url: Option<&'a str>,
    clock: Option<&'a str>,
    vantage: Option<&'a str>,
    software: Option<&'a str>,
    method_id: Option<QualifiedRef>,
    public_key: Option<&'a [u8; 32]>,
    route_class: Option<&'a str>,
    software_version: Option<&'a str>,
    label: Option<&'a str>,
    visibility_scope_id: Option<QualifiedRef>,
    input: Option<&'a str>,
    algorithm: Option<&'a str>,
    capability: Option<&'a str>,
    output_unit: Option<&'a str>,
    failure_taxonomy: Option<&'a str>,
    algorithm_version: Option<u16>,
    protocol_revision: Option<&'a str>,
    format: Option<&'a str>,
    format_version: Option<u16>,
    source_dataset_id: Option<&'a [u8; 16]>,
    source_artifact_digest: Option<&'a [u8; 32]>,
}
impl<'a> Descriptor<'a> {
    fn texts(&self) -> [Option<&str>; 15] {
        [
            self.class,
            self.url,
            self.clock,
            self.vantage,
            self.software,
            self.route_class,
            self.software_version,
            self.label,
            self.input,
            self.algorithm,
            self.capability,
            self.output_unit,
            self.failure_taxonomy,
            self.protocol_revision,
            self.format,
        ]
    }
    fn of(value: &'a DictionaryDescriptor) -> Self {
        use DictionaryDescriptor as D;
        match value {
            D::Event(id) => Self {
                event_id: Some(id.as_bytes()),
                ..Self::default()
            },
            D::Visibility { class } => Self {
                class: Some(class.as_str()),
                ..Self::default()
            },
            D::Endpoint(value) => Self {
                url: Some(value.url()),
                ..Self::default()
            },
            D::Observer(value) => Self {
                clock: value.clock.map(|v| v.as_str()),
                vantage: value.vantage.as_ref().map(|v| v.as_str()),
                software: value.software.as_ref().map(|v| v.as_str()),
                method_id: value.method_id,
                public_key: value.public_key.as_ref().map(|v| v.as_bytes()),
                route_class: value.route_class.map(|v| v.as_str()),
                software_version: value.software_version.as_ref().map(|v| v.as_str()),
                ..Self::default()
            },
            D::AccessScope(value) => Self {
                class: Some(value.class.as_str()),
                label: value.label.as_ref().map(|v| v.as_str()),
                visibility_scope_id: Some(value.visibility_scope_id),
                ..Self::default()
            },
            D::EventVerification => Self {
                input: Some("nip01-event/1"),
                algorithm: Some("sha256-bip340"),
                capability: Some("event-id-and-signature"),
                output_unit: Some("verification-result"),
                failure_taxonomy: Some("bigbrotr-nip01-verifier/1"),
                algorithm_version: Some(1),
                protocol_revision: Some(
                    bigbrotr_model::dictionary::EVENT_VERIFICATION_NIPS_REVISION,
                ),
                ..Self::default()
            },
            D::DatasetLineage(value) => Self {
                format: Some(value.format.as_str()),
                format_version: Some(value.format_version),
                source_dataset_id: value.source_dataset_id.as_ref().map(|v| v.as_bytes()),
                source_artifact_digest: Some(value.source_artifact_digest.as_bytes()),
                ..Self::default()
            },
        }
    }
}

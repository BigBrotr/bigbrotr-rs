//! Exact original A05 hydration under a complete caller-authorized pinned cut.
//! Caller retains current scope authority, the actual pin and catalog barrier.
use super::{
    CatalogReaderCredentials, CatalogRoute, Preparation, PreparationError, PreparationLimits,
    ProcessError, ReaderCustody, ReceiptError, WorkerBudgets,
};
use crate::{
    runtime::{OwnedPermit, ResourceRequest},
    storage::postgres::catalog_guard::CatalogIdentity,
};
use bigbrotr_model::Fact;
use bigbrotr_protocol::{
    archive_cut::CutObservation,
    archive_measurement::{
        MAX_ROW_BYTES, MeasurementObservation, MeasurementRead, MeasurementReadError,
    },
    archive_receipt::MAX_CHUNK_ROWS,
};
use serde::Serialize;
use std::{sync::atomic::Ordering, time::Instant};

/// Covers retained cut metadata, bounded wire, fixed decoded row/fact and decoder
/// overlap. Native engine/vectors have the separate checked job reservation.
pub const MEASUREMENT_DATA_BYTES: u64 = 1 << 20;
const OUTPUT_BYTES: usize = MAX_ROW_BYTES + 2048;

/// One exact immutable fact; inline children cannot become independent votes.
/// No constructor accepts a caller's purported observation or pin.
pub struct ArchivedMeasurement {
    read: MeasurementRead,
    cut: CutObservation,
    fact: Fact,
    _permit: OwnedPermit,
    _custody: ReaderCustody,
}
impl ArchivedMeasurement {
    pub fn read(&self) -> &MeasurementRead {
        &self.read
    }
    pub fn cut(&self) -> &CutObservation {
        &self.cut
    }
    pub fn fact(&self) -> &Fact {
        &self.fact
    }
    pub fn catalog_identity(&self) -> &CatalogIdentity {
        self._custody.identity()
    }
}
#[derive(Debug)]
pub enum ArchiveMeasurementError {
    Input(MeasurementReadError),
    Reader(ReceiptError),
    ResourceLimit,
    Preparation(PreparationError),
    ObservationUnknown,
    TerminationUnknown,
}

impl std::fmt::Display for ArchiveMeasurementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "archive measurement read failed: {self:?}")
    }
}
impl std::error::Error for ArchiveMeasurementError {}

/// Validate the entire cut/receipt census and overlapping paired fixed A05 rows
/// before launching. This is allocation admission, not scan/RSS qualification.
pub fn validate_measurement_limits(
    limits: PreparationLimits,
    read: &MeasurementRead,
    max_facts: usize,
) -> Result<(), ArchiveMeasurementError> {
    read.validate(max_facts)
        .map_err(ArchiveMeasurementError::Input)?;
    let records = read
        .cut
        .validate(max_facts)
        .map_err(|_| ArchiveMeasurementError::ObservationUnknown)?;
    let rows = records.saturating_add(1).min(MAX_CHUNK_ROWS as u64);
    // Native JSON, Arrow UTF-8 and Rust typed decode may overlap for both current
    // and prior rows. The original membership/receipt floor remains additive.
    let overlap = rows
        .checked_mul(6 * (MAX_ROW_BYTES as u64 + 1))
        .ok_or(ArchiveMeasurementError::ResourceLimit)?;
    let available = limits
        .native_reserve_bytes
        .checked_sub(overlap)
        .ok_or(ArchiveMeasurementError::ResourceLimit)?;
    super::validate_cut_limits(
        PreparationLimits {
            native_reserve_bytes: available,
            ..limits
        },
        &read.cut,
        max_facts,
    )
    .map_err(ArchiveMeasurementError::Reader)
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: &'a MeasurementRead,
}
fn prepare_input(
    read: &MeasurementRead,
    identity: &CatalogIdentity,
    route: &CatalogRoute<'_>,
    credential: CatalogReaderCredentials<'_>,
    allow_fd_growth: bool,
) -> Result<
    (
        zeroize::Zeroizing<String>,
        Option<super::client_identity::NativeTlsHandoff>,
    ),
    ArchiveMeasurementError,
> {
    read.validate(usize::MAX)
        .map_err(ArchiveMeasurementError::Input)?;
    let (dsn, tls) = super::receipts::reader_connection(
        read.cut.dataset_id,
        identity,
        route,
        credential,
        allow_fd_growth,
        "bigbrotr-archive-measurement",
    )
    .map_err(ArchiveMeasurementError::Reader)?;
    let input = Input {
        catalog: super::bootstrap::Input {
            dsn: &dsn,
            data_root: identity.root(),
        },
        catalog_id: identity.catalog(),
        expected: read,
    };
    input
        .catalog
        .validate()
        .map_err(ArchiveMeasurementError::Preparation)?;
    super::receipts::wire_fits(&input).map_err(ArchiveMeasurementError::Reader)?;
    Ok((dsn, tls))
}
impl CatalogRoute<'_> {
    /// Pure full-wire preflight. No policy, snapshot pin or I/O authority follows.
    pub fn validate_measurement(
        &self,
        read: &MeasurementRead,
        identity: &CatalogIdentity,
        credential: CatalogReaderCredentials<'_>,
    ) -> Result<(), ArchiveMeasurementError> {
        prepare_input(read, identity, self, credential, true).map(|_| ())
    }
}
impl Preparation<'_> {
    /// The caller constructs `read.cut` from its actual bound published inputs,
    /// holds the matching pin and current read authority, and stabilizes the
    /// catalog through this joined operation. None/latest snapshots are refused.
    pub fn read_measurement(
        self,
        read: MeasurementRead,
        custody: ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budgets: WorkerBudgets<'_>,
        max_facts: usize,
    ) -> Result<ArchivedMeasurement, ArchiveMeasurementError> {
        validate_measurement_limits(self.limits, &read, max_facts)?;
        let (dsn, tls) = prepare_input(&read, custody.identity(), route, credential, false)?;
        let input = Input {
            catalog: super::bootstrap::Input {
                dsn: &dsn,
                data_root: custody.identity().root(),
            },
            catalog_id: custody.identity().catalog(),
            expected: &read,
        };
        let permit = budgets
            .application
            .try_acquire(
                ResourceRequest::data(MEASUREMENT_DATA_BYTES, 1, 0)
                    .map_err(|_| ArchiveMeasurementError::ResourceLimit)?,
            )
            .map_err(|_| ArchiveMeasurementError::ResourceLimit)?;
        let mut process = self
            .start_reader(
                "read-measurement",
                &[self.limits.max_records.to_string(), max_facts.to_string()],
                budgets,
                &custody,
                tls,
            )
            .map_err(ArchiveMeasurementError::Preparation)?;
        if process.reserve_data_output(OUTPUT_BYTES, permit).is_err() {
            if matches!(process.finish(), Err(ProcessError::TerminationUnknown)) {
                return Err(ArchiveMeasurementError::TerminationUnknown);
            }
            return Err(ArchiveMeasurementError::ResourceLimit);
        }
        let transfer = serde_json::to_writer(&mut process, &input);
        let result = process.take_data_output();
        if matches!(result, Err(ProcessError::TerminationUnknown)) {
            return Err(ArchiveMeasurementError::TerminationUnknown);
        }
        if transfer.is_err() {
            return Err(ArchiveMeasurementError::ObservationUnknown);
        }
        let (bytes, permit) = result.map_err(|error| match error {
            ProcessError::ResourceLimit => ArchiveMeasurementError::ResourceLimit,
            _ => ArchiveMeasurementError::ObservationUnknown,
        })?;
        let (fact, cut) = decode_response(&bytes, &read, max_facts)?;
        if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            return Err(ArchiveMeasurementError::ObservationUnknown);
        }
        Ok(ArchivedMeasurement {
            read,
            cut,
            fact,
            _permit: permit,
            _custody: custody,
        })
    }
}
fn decode_response(
    bytes: &[u8],
    read: &MeasurementRead,
    max_facts: usize,
) -> Result<(Fact, CutObservation), ArchiveMeasurementError> {
    if bytes.len() > OUTPUT_BYTES {
        return Err(ArchiveMeasurementError::ResourceLimit);
    }
    let observation: MeasurementObservation =
        serde_json::from_slice(bytes).map_err(|_| ArchiveMeasurementError::ObservationUnknown)?;
    let fact = observation
        .validate(read, max_facts)
        .map_err(|_| ArchiveMeasurementError::ObservationUnknown)?;
    Ok((fact, observation.cut))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_model::codec::FactDecodeLimits;
    use bigbrotr_protocol::archive_measurement::MeasurementFactRow;

    fn read_and_fact() -> (MeasurementRead, Fact) {
        let cuts: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/protocol/archive-cut.json"
        ))
        .unwrap();
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../crates/bigbrotr-model/tests/fixtures/measurement-v1.json"
        ))
        .unwrap();
        let literal = &vectors["valid"][0];
        let bytes: Vec<_> = literal["preimage_hex"]
            .as_str()
            .unwrap()
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect();
        let fact = Fact::decode_preimage(
            &bytes,
            literal["sha256"].as_str().unwrap().parse().unwrap(),
            FactDecodeLimits::default(),
        )
        .unwrap();
        let cut = serde_json::from_value(cuts["read"].clone()).unwrap();
        (
            MeasurementRead {
                cut,
                fact: fact.fact_ref(),
            },
            fact,
        )
    }
    fn limits() -> PreparationLimits {
        PreparationLimits {
            memory_bytes: 128 << 20,
            temp_bytes: 64 << 20,
            threads: 1,
            max_records: 128,
            max_encoded_bytes: 1 << 20,
            native_reserve_bytes: 384 << 20,
        }
    }

    #[test]
    fn exact_complete_cut_preflight_distinguishes_missing_pin_source_and_budget() {
        let (read, _) = read_and_fact();
        validate_measurement_limits(limits(), &read, 9).unwrap();
        let mut bad = read.clone();
        bad.cut.snapshot_id = None;
        assert!(matches!(
            validate_measurement_limits(limits(), &bad, 9),
            Err(ArchiveMeasurementError::Input(
                MeasurementReadError::InvalidInput
            ))
        ));
        bad = read.clone();
        bad.fact.source_record_id.shard_id = 99;
        assert!(matches!(
            validate_measurement_limits(limits(), &bad, 9),
            Err(ArchiveMeasurementError::Input(
                MeasurementReadError::InvalidInput
            ))
        ));
        assert!(validate_measurement_limits(limits(), &read, 8).is_err());
        assert!(
            validate_measurement_limits(
                PreparationLimits {
                    native_reserve_bytes: 1,
                    ..limits()
                },
                &read,
                9
            )
            .is_err()
        );
        let route = CatalogRoute {
            address: "127.0.0.1".parse().unwrap(),
            port: 443,
            database: "archive",
            tls: super::super::CatalogTls::LocalPlaintext(
                crate::storage::postgres::connection_settings::PlaintextBoundary::Loopback,
            ),
        };
        let identity = CatalogIdentity::local(
            read.cut.dataset_id,
            [90; 16].try_into().unwrap(),
            "/archive/",
        )
        .unwrap();
        route
            .validate_measurement(
                &read,
                &identity,
                CatalogReaderCredentials {
                    login: "reader",
                    password: "fixture-only",
                },
            )
            .unwrap();
    }
    #[test]
    fn decoded_original_requires_exact_cut_fact_and_positive_measurement_count() {
        // Private decode controls only: these synthetic structural replies cannot
        // manufacture ArchivedMeasurement, custody, a native proof or read pin.
        let (read, fact) = read_and_fact();
        let response = MeasurementObservation {
            format: bigbrotr_protocol::archive_measurement::FORMAT.into(),
            cut: CutObservation {
                format: bigbrotr_protocol::archive_cut::FORMAT.into(),
                snapshot_id: read.cut.snapshot_id.unwrap(),
                request_digest: read.cut.digest(),
                family_counts: [8, 0, 0, 0, 0, 1],
            },
            fact: MeasurementFactRow::from_fact(&fact).unwrap(),
        };
        let bytes = serde_json::to_vec(&response).unwrap();
        assert_eq!(decode_response(&bytes, &read, 9).unwrap().0, fact);
        for change in 0..6 {
            let mut bad = response.clone();
            match change {
                0 => bad.cut.snapshot_id += 1,
                1 => bad.cut.request_digest = bigbrotr_model::Digest256::from_bytes([0; 32]),
                2 => bad.fact.payload.profile[13] += 1,
                3 => bad.fact.payload_digest = bigbrotr_model::Digest256::from_bytes([0; 32]),
                4 => bad.cut.family_counts = [9, 0, 0, 0, 0, 0],
                5 => bad.format = "bigbrotr-native-measurement/2".into(),
                _ => unreachable!(),
            }
            assert!(
                decode_response(&serde_json::to_vec(&bad).unwrap(), &read, 9).is_err(),
                "change {change}"
            );
        }
        for incomplete in [b"".as_slice(), b"{}", b"null"] {
            assert!(decode_response(incomplete, &read, 9).is_err());
        }
    }
}

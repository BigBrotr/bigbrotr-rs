//! Finite identity/file census. Does not acquire a source cut or protection pin.
use super::{
    CatalogReaderCredentials, CatalogRoute, Preparation, PreparationLimits, ProcessError,
    ReceiptError, receipts,
};
use crate::{runtime::ResourceBudget, storage::postgres::catalog_guard::CatalogIdentity};
use bigbrotr_protocol::{
    archive_membership::{MAX_PATH_BYTES, MembershipObservation, MembershipRead},
    archive_receipt::{ExpectationError, MAX_CHUNK_ROWS},
};
use serde::Serialize;
use std::{sync::atomic::Ordering, time::Instant};

fn input_error(error: ExpectationError) -> ReceiptError {
    match error {
        ExpectationError::InvalidIdentity => ReceiptError::InvalidInput,
        ExpectationError::ResourceLimit => ReceiptError::ResourceLimit,
    }
}
pub fn validate_membership_limits(
    limits: PreparationLimits,
    read: &MembershipRead,
    max_facts: usize,
) -> Result<(), ReceiptError> {
    read.validate(max_facts).map_err(input_error)?;
    // Original and regrouped FactRef buffers overlap. Arrow/native conversion
    // may hold three vectors; character-clamped filenames can use four UTF-8
    // bytes each. Receipt scanning has a separate floor, conservatively added.
    let records = read.receipt.record_count;
    let rows = records.saturating_add(1).min(MAX_CHUNK_ROWS as u64);
    let receipt_floor = receipts::receipt_allocation_floor(limits)?;
    let floor = records
        .checked_mul(2 * std::mem::size_of::<bigbrotr_model::FactRef>() as u64)
        .and_then(|n| n.checked_add(rows * 3 * (4 * (MAX_PATH_BYTES as u64 + 1) + 256)))
        .and_then(|n| n.checked_add(16 * 1024 * 1024))
        .and_then(|n| n.checked_add(receipt_floor))
        .ok_or(ReceiptError::ResourceLimit)?;
    if limits.native_reserve_bytes < floor {
        return Err(ReceiptError::ResourceLimit);
    }
    Ok(())
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: &'a MembershipRead,
}
impl Preparation<'_> {
    pub fn inspect_membership(
        self,
        expected: &MembershipRead,
        identity: &CatalogIdentity,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
        max_facts: usize,
    ) -> Result<MembershipObservation, ReceiptError> {
        validate_membership_limits(self.limits, expected, max_facts)?;
        let (dsn, tls) = receipts::reader_connection(
            expected.receipt.dataset_id,
            identity,
            route,
            credential,
            false,
            "bigbrotr-archive-membership",
        )?;
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
            .map_err(ReceiptError::Preparation)?;
        receipts::wire_fits(&input)?;
        let mut process = self
            .start_with_custody(
                "inspect-membership",
                &[self.limits.max_records.to_string(), max_facts.to_string()],
                budget,
                None,
                tls,
            )
            .map_err(ReceiptError::Preparation)?;
        let transfer = serde_json::to_writer(&mut process, &input);
        let output = process.finish();
        if matches!(output, Err(ProcessError::TerminationUnknown)) {
            return Err(ReceiptError::TerminationUnknown);
        }
        if transfer.is_err()
            || self.cancelled.load(Ordering::Relaxed)
            || Instant::now() >= self.deadline
        {
            return Err(ReceiptError::ObservationUnknown);
        }
        decode_response(
            output.map_err(|_| ReceiptError::ObservationUnknown)?,
            expected,
        )
    }
}
fn decode_response(
    bytes: &[u8],
    read: &MembershipRead,
) -> Result<MembershipObservation, ReceiptError> {
    let observation: MembershipObservation =
        serde_json::from_slice(bytes).map_err(|_| ReceiptError::ObservationUnknown)?;
    observation
        .validate(read)
        .map_err(|_| ReceiptError::ObservationUnknown)?;
    Ok(observation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_protocol::archive_membership::FORMAT;

    fn read() -> MembershipRead {
        let v: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/protocol/archive-membership.json"
        ))
        .unwrap();
        serde_json::from_value(v["read"].clone()).unwrap()
    }

    #[test]
    fn member_admission_preserves_invalid_limits_and_funds_actual_batch() {
        let read = read();
        let mut limits = PreparationLimits {
            memory_bytes: 128 << 20,
            temp_bytes: 64 << 20,
            threads: 1,
            max_records: 128,
            max_encoded_bytes: 1 << 20,
            native_reserve_bytes: 384 << 20,
        };
        assert_eq!(validate_membership_limits(limits, &read, 4), Ok(()));
        assert_eq!(
            validate_membership_limits(limits, &read, usize::MAX),
            Ok(())
        );
        assert_eq!(
            validate_membership_limits(limits, &read, 3),
            Err(ReceiptError::ResourceLimit)
        );
        limits.max_records = 0;
        assert_eq!(
            validate_membership_limits(limits, &read, 4),
            Err(ReceiptError::InvalidInput)
        );
        limits.max_records = usize::MAX;
        assert_eq!(
            validate_membership_limits(limits, &read, 4),
            Err(ReceiptError::InvalidInput)
        );
        limits.max_records = 128;
        limits.native_reserve_bytes = receipts::receipt_allocation_floor(limits).unwrap();
        assert_eq!(
            validate_membership_limits(limits, &read, 4),
            Err(ReceiptError::ResourceLimit)
        );
    }

    #[test]
    fn member_response_is_bound_to_identity_snapshot_and_complete_counts() {
        let read = read();
        let expected = MembershipObservation {
            format: FORMAT.into(),
            snapshot_id: 2,
            batch_digest: read.receipt.batch_digest,
            attempt_id: read.receipt.attempt_id,
            publication_ordinal: 1,
            family_counts: [2, 1, 0, 1, 0],
            files: 4,
        };
        let reply = serde_json::to_vec(&expected).unwrap();
        assert_eq!(decode_response(&reply, &read), Ok(expected.clone()));
        for case in [
            "snapshot", "digest", "attempt", "ordinal", "counts", "overflow", "files", "format",
        ] {
            let mut altered = expected.clone();
            match case {
                "snapshot" => altered.snapshot_id = 3,
                "digest" => altered.batch_digest = bigbrotr_model::Digest256::from_bytes([2; 32]),
                "attempt" => altered.attempt_id = [8; 16],
                "ordinal" => altered.publication_ordinal = 2,
                "counts" => altered.family_counts[0] = 3,
                "overflow" => altered.family_counts = [u64::MAX, 5, 0, 0, 0],
                "files" => altered.files = 3,
                "format" => altered.format = "bigbrotr-native-receipt/1".into(),
                _ => unreachable!(),
            }
            assert_eq!(
                decode_response(&serde_json::to_vec(&altered).unwrap(), &read),
                Err(ReceiptError::ObservationUnknown),
                "{case}"
            );
        }
        let reply = String::from_utf8(reply).unwrap();
        for invalid in [
            format!("{reply}{{}}"),
            format!("{},\"snapshot_id\":2}}", &reply[..reply.len() - 1]),
            format!("{},\"authority\":true}}", &reply[..reply.len() - 1]),
        ] {
            assert_eq!(
                decode_response(invalid.as_bytes(), &read),
                Err(ReceiptError::ObservationUnknown)
            );
        }
    }
}

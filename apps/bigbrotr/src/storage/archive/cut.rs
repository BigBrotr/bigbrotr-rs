//! One finite cut observation; no topology, persistence or retention authority.
use super::{
    CatalogReaderCredentials, CatalogRoute, Preparation, PreparationLimits, ProcessError,
    ReceiptError, WorkerBudgets, membership, receipts,
};
use crate::{
    runtime::{OwnedPermit, ResourceRequest},
    storage::postgres::catalog_guard::CatalogIdentity,
};
use bigbrotr_protocol::{
    archive_cut::{CutObservation, CutRead},
    archive_receipt::ExpectationError,
};
use serde::Serialize;
use std::{sync::atomic::Ordering, time::Instant};

/// Actual joined native observation. Construction is private; this is still not
/// topology authority, qualified persistence, a protected frontier or a read pin.
/// The input and allocation grant remain retained through the downstream consumer.
pub struct ObservedCut {
    read: CutRead,
    observation: CutObservation,
    catalog: CatalogIdentity,
    started: Instant,
    _permit: OwnedPermit,
}
impl ObservedCut {
    pub fn read(&self) -> &CutRead {
        &self.read
    }
    pub fn observation(&self) -> &CutObservation {
        &self.observation
    }
    pub fn catalog(&self) -> &CatalogIdentity {
        &self.catalog
    }
    pub(crate) fn started(&self) -> Instant {
        self.started
    }
}
fn input_error(error: ExpectationError) -> ReceiptError {
    match error {
        ExpectationError::InvalidIdentity => ReceiptError::InvalidInput,
        ExpectationError::ResourceLimit => ReceiptError::ResourceLimit,
    }
}
pub fn validate_cut_limits(
    limits: PreparationLimits,
    read: &CutRead,
    max_facts: usize,
) -> Result<(), ReceiptError> {
    let records = read.validate(max_facts).map_err(input_error)?;
    membership::validate_member_count_limits(limits, records)?;
    if read.receipts.len() > limits.max_records {
        return Err(ReceiptError::ResourceLimit);
    }
    Ok(())
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: &'a CutRead,
}
impl Preparation<'_> {
    /// One native transaction for all supplied complete inputs. The caller holds
    /// real custody and catalog stabilization throughout observation and binding.
    pub fn inspect_cut(
        self,
        expected: CutRead,
        custody: super::ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budgets: WorkerBudgets<'_>,
        max_facts: usize,
    ) -> Result<ObservedCut, ReceiptError> {
        let started = Instant::now();
        let identity = custody.identity();
        validate_cut_limits(self.limits, &expected, max_facts)?;
        let grant = budgets
            .application
            .try_acquire(
                ResourceRequest::data(1 << 20, 1, 0).map_err(|_| ReceiptError::ResourceLimit)?,
            )
            .map_err(|_| ReceiptError::ResourceLimit)?;
        let (dsn, tls) = receipts::reader_connection(
            expected.dataset_id,
            identity,
            route,
            credential,
            false,
            "bigbrotr-archive-cut",
        )?;
        let input = Input {
            catalog: super::bootstrap::Input {
                dsn: &dsn,
                data_root: identity.root(),
            },
            catalog_id: identity.catalog(),
            expected: &expected,
        };
        input
            .catalog
            .validate()
            .map_err(ReceiptError::Preparation)?;
        receipts::wire_fits(&input)?;
        let mut process = self
            .start_reader(
                "inspect-cut",
                &[self.limits.max_records.to_string(), max_facts.to_string()],
                budgets,
                &custody,
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
        let observation: CutObservation =
            serde_json::from_slice(output.map_err(|_| ReceiptError::ObservationUnknown)?)
                .map_err(|_| ReceiptError::ObservationUnknown)?;
        observation
            .validate(&expected, max_facts)
            .map_err(|_| ReceiptError::ObservationUnknown)?;
        Ok(ObservedCut {
            read: expected,
            observation,
            catalog: identity.clone(),
            started,
            _permit: grant,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr_protocol::archive_cut::CutRead;
    fn read() -> CutRead {
        let v: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/protocol/archive-cut.json"
        ))
        .unwrap();
        serde_json::from_value(v["read"].clone()).unwrap()
    }
    #[test]
    fn cut_admission_charges_all_records_and_keeps_input_errors_distinct() {
        let mut limits = PreparationLimits {
            memory_bytes: 128 << 20,
            temp_bytes: 64 << 20,
            threads: 1,
            max_records: 128,
            max_encoded_bytes: 1 << 20,
            native_reserve_bytes: 384 << 20,
        };
        assert_eq!(validate_cut_limits(limits, &read(), 9), Ok(()));
        assert_eq!(
            validate_cut_limits(limits, &read(), 8),
            Err(ReceiptError::ResourceLimit)
        );
        limits.max_records = 1;
        assert_eq!(
            validate_cut_limits(limits, &read(), 9),
            Err(ReceiptError::ResourceLimit)
        );
        limits.max_records = 0;
        assert_eq!(
            validate_cut_limits(limits, &read(), 9),
            Err(ReceiptError::InvalidInput)
        );
        limits.max_records = 128;
        limits.native_reserve_bytes = 1;
        assert_eq!(
            validate_cut_limits(limits, &read(), 9),
            Err(ReceiptError::ResourceLimit)
        );
    }
}

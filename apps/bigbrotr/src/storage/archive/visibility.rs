//! Allocation evidence for a future grant, never data/action authorization.
use super::{
    CatalogReaderCredentials, CatalogRoute, Preparation, ProcessError, ReaderCustody, ReceiptError,
};
use crate::{
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
    storage::postgres::catalog_guard::CatalogIdentity,
};
use bigbrotr_model::{DictionaryEntry, Fact, FactPayload, FactRef};
use bigbrotr_protocol::archive_visibility::{
    MAX_FACT_BYTES, MAX_NATIVE_ROW_BYTES, MAX_RESPONSE_BYTES, VisibilityObservation,
    VisibilityRead, VisibilityReadError,
};
use serde::Serialize;
use std::{sync::atomic::Ordering, time::Instant};

/// Only a joined real reader constructs this value. It proves one stored mapping
/// in the supplied selection, not certification of that cut or permission to
/// read its data. The Control consumer must match its certified pin and dataset.
pub struct ArchivedVisibility {
    fact: Fact,
    read: VisibilityRead,
    _permit: OwnedPermit,
    custody: ReaderCustody,
}
impl ArchivedVisibility {
    pub fn entry(&self) -> &DictionaryEntry {
        match self.fact.payload() {
            FactPayload::Dictionary(entry) => entry,
            _ => unreachable!("verified visibility fact"),
        }
    }
    pub fn fact_ref(&self) -> FactRef {
        self.fact.fact_ref()
    }
    pub fn read(&self) -> &VisibilityRead {
        &self.read
    }
    pub fn catalog(&self) -> &CatalogIdentity {
        self.custody.identity()
    }
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: &'a VisibilityRead,
}
impl Preparation<'_> {
    /// The caller retains the certified snapshot pin and stabilizes the catalog.
    /// Parsed QualifiedRef/class fields alone never create this observation.
    pub fn read_visibility(
        self,
        expected: VisibilityRead,
        custody: ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
    ) -> Result<ArchivedVisibility, ReceiptError> {
        expected.validate().map_err(|error| match error {
            VisibilityReadError::ResourceLimit => ReceiptError::ResourceLimit,
            _ => ReceiptError::InvalidInput,
        })?;
        // Two clamped native rows, Arrow/UTF-8 conversion, request bounds and
        // canonical verification overlap. Engine memory/OS limits stay separate.
        const OVERLAP: u64 = 64 * MAX_NATIVE_ROW_BYTES as u64;
        if self.limits.max_encoded_bytes < MAX_FACT_BYTES
            || self.limits.native_reserve_bytes < 16 * 1024 * 1024 + OVERLAP
        {
            return Err(ReceiptError::ResourceLimit);
        }
        let grant = budget
            .try_acquire(
                ResourceRequest::data(OVERLAP, 1, 0).map_err(|_| ReceiptError::ResourceLimit)?,
            )
            .map_err(|_| ReceiptError::ResourceLimit)?;
        let identity = custody.identity();
        let (dsn, tls) = super::receipts::reader_connection(
            expected.dataset,
            identity,
            route,
            credential,
            false,
            "bigbrotr-archive-visibility",
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
        super::bootstrap::wire_fits(&input).map_err(ReceiptError::Preparation)?;
        let mut process = self
            .start_reader("read-visibility", &[], budget, &custody, tls)
            .map_err(ReceiptError::Preparation)?;
        let transfer = serde_json::to_writer(&mut process, &input);
        let output = process.finish();
        if matches!(output, Err(ProcessError::TerminationUnknown)) {
            return Err(ReceiptError::TerminationUnknown);
        }
        if transfer.is_err() {
            return Err(ReceiptError::ObservationUnknown);
        }
        let output = output.map_err(|_| ReceiptError::ObservationUnknown)?;
        if output.len() > MAX_RESPONSE_BYTES {
            return Err(ReceiptError::ObservationUnknown);
        }
        let observation: VisibilityObservation =
            serde_json::from_slice(output).map_err(|_| ReceiptError::ObservationUnknown)?;
        let fact = observation
            .verify(&expected)
            .map_err(|_| ReceiptError::ObservationUnknown)?;
        if self.cancelled.load(Ordering::Relaxed) || Instant::now() >= self.deadline {
            return Err(ReceiptError::ObservationUnknown);
        }
        Ok(ArchivedVisibility {
            fact,
            read: expected,
            _permit: grant,
            custody,
        })
    }
}

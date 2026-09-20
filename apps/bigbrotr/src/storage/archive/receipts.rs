//! Exact fresh receipt observation, without granting replay or source protection.
use super::{CatalogRoute, Preparation, PreparationError, ProcessError};
use crate::{runtime::ResourceBudget, storage::postgres::catalog_guard::CatalogIdentity};
use bigbrotr_protocol::archive_receipt::{
    ExpectationError, MAX_CHUNK_ROWS, MAX_INPUT_BYTES, MAX_ROW_BYTES, ReceiptExpectation,
};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

/// Operator-provisioned SELECT-only catalog login. Never put this in diagnostics.
pub struct CatalogReaderCredentials<'a> {
    pub login: &'a str,
    pub password: &'a str,
}

#[cfg(test)]
#[path = "receipts_tests.rs"]
mod tests;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReceiptObservation {
    Absent,
    Present,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptError {
    InvalidInput,
    ResourceLimit,
    Preparation(PreparationError),
    ObservationUnknown,
    TerminationUnknown,
}
#[derive(Serialize)]
struct Input<'a> {
    catalog: super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: &'a ReceiptExpectation,
}
struct Size(usize);
impl Write for Size {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|v| *v <= MAX_INPUT_BYTES)
            .ok_or_else(|| io::Error::other("receipt input limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn wire_fits(input: &impl Serialize) -> Result<(), ReceiptError> {
    serde_json::to_writer(&mut Size(0), input).map_err(|_| ReceiptError::ResourceLimit)
}
impl CatalogRoute<'_> {
    /// Validate the exact complete reader request before issuing an Archive
    /// attempt. No filesystem or network I/O and no credential persistence.
    /// Native grants, source retention and publication eligibility remain separate.
    pub fn validate_receipt(
        &self,
        expected: &ReceiptExpectation,
        identity: &CatalogIdentity,
        credential: CatalogReaderCredentials<'_>,
    ) -> Result<(), ReceiptError> {
        prepare_input(expected, identity, self, credential, true).map(|_| ())
    }
}
fn prepare_input(
    expected: &ReceiptExpectation,
    identity: &CatalogIdentity,
    route: &CatalogRoute<'_>,
    credential: CatalogReaderCredentials<'_>,
    allow_fd_growth: bool,
) -> Result<
    (
        zeroize::Zeroizing<String>,
        Option<super::client_identity::NativeTlsHandoff>,
    ),
    ReceiptError,
> {
    expected.validate().map_err(|error| match error {
        ExpectationError::InvalidIdentity => ReceiptError::InvalidInput,
        ExpectationError::ResourceLimit => ReceiptError::ResourceLimit,
    })?;
    let (dsn, tls) = reader_connection(
        expected.dataset_id,
        identity,
        route,
        credential,
        allow_fd_growth,
        "bigbrotr-archive-receipt",
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
    wire_fits(&input)?;
    Ok((dsn, tls))
}
pub(super) fn reader_connection(
    dataset: bigbrotr_model::DatasetId,
    identity: &CatalogIdentity,
    route: &CatalogRoute<'_>,
    credential: CatalogReaderCredentials<'_>,
    allow_fd_growth: bool,
    application: &str,
) -> Result<
    (
        zeroize::Zeroizing<String>,
        Option<super::client_identity::NativeTlsHandoff>,
    ),
    ReceiptError,
> {
    let scientific = CatalogIdentity::local(dataset, identity.catalog(), identity.root())
        .map_err(|_| ReceiptError::InvalidInput)?;
    if identity != &scientific {
        return Err(ReceiptError::InvalidInput);
    }
    if [credential.login, credential.password]
        .iter()
        .any(|s| s.is_empty() || s.len() > 4096 || s.chars().any(char::is_control))
    {
        return Err(ReceiptError::InvalidInput);
    }
    let (mut dsn, tls) = route
        .connection(credential.login, credential.password, application)
        .map_err(|_| ReceiptError::InvalidInput)?;
    // libpq applies this to every connection, including native attach work.
    // Explicit READ_ONLY attachments and SELECT-only grants are separate.
    dsn.push_str(" options='-c default_transaction_read_only=on'");
    if allow_fd_growth && let Some(handoff) = &tls {
        // Trailing spaces size both the raw DSN and its JSON for the largest
        // possible later FD numbers. This preflight-only value is never sent.
        dsn.extend(std::iter::repeat_n(' ', handoff.maximum_parameter_growth()));
    }
    Ok((dsn, tls))
}
/// Pure bounded scan admission shared by operator preflight and actual launch.
pub fn validate_receipt_limits(limits: super::PreparationLimits) -> Result<(), ReceiptError> {
    if limits.native_reserve_bytes < receipt_allocation_floor(limits)? {
        return Err(ReceiptError::ResourceLimit);
    }
    Ok(())
}
pub(super) fn receipt_allocation_floor(
    limits: super::PreparationLimits,
) -> Result<u64, ReceiptError> {
    if limits.max_records == 0 || limits.max_records.checked_add(1).is_none() {
        return Err(ReceiptError::InvalidInput);
    }
    // One native vector plus Arrow/string conversion can coexist. This is
    // an application allocation floor, not an engine/whole-process RSS cap.
    let rows = limits.max_records.saturating_add(1).min(MAX_CHUNK_ROWS) as u64;
    Ok(16 * 1024 * 1024 + rows * 3 * (MAX_ROW_BYTES as u64 + 128))
}
impl Preparation<'_> {
    /// A new native process/transaction observes the original retained attempt.
    /// Absence is not exclusion: callers must separately fence every old writer
    /// and resolve its transactions before interpreting this as replay evidence.
    /// Present verifies the receipt only, not all scientific objects or durability.
    pub fn inspect_receipt(
        self,
        expected: &ReceiptExpectation,
        identity: &CatalogIdentity,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budget: &ResourceBudget,
    ) -> Result<ReceiptObservation, ReceiptError> {
        validate_receipt_limits(self.limits)?;
        let (dsn, tls) = prepare_input(expected, identity, route, credential, false)?;
        let input = Input {
            catalog: super::bootstrap::Input {
                dsn: &dsn,
                data_root: identity.root(),
            },
            catalog_id: identity.catalog(),
            expected,
        };
        let mut process = self
            .start_with_custody(
                "inspect-receipt",
                &[self.limits.max_records.to_string()],
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
        if transfer.is_err() {
            return Err(ReceiptError::ObservationUnknown);
        }
        decode_response(
            output.map_err(|_| ReceiptError::ObservationUnknown)?,
            expected,
        )
    }
}
fn decode_response(
    output: &[u8],
    expected: &ReceiptExpectation,
) -> Result<ReceiptObservation, ReceiptError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Response {
        format: String,
        status: ReceiptObservation,
        batch_digest: bigbrotr_model::Digest256,
        attempt_id: [u8; 16],
        publication_ordinal: u64,
    }
    let value: Response =
        serde_json::from_slice(output).map_err(|_| ReceiptError::ObservationUnknown)?;
    if value.format != "bigbrotr-native-receipt/1"
        || value.batch_digest != expected.batch_digest
        || value.attempt_id != expected.attempt_id
        || value.publication_ordinal != expected.publication_ordinal
    {
        return Err(ReceiptError::ObservationUnknown);
    }
    Ok(value.status)
}

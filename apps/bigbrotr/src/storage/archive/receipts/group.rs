//! Retained-only joined observation of one complete group of issued alternatives.
use super::{
    CatalogReaderCredentials, ReceiptError, reader_connection, validate_receipt_limits, wire_fits,
};
use crate::storage::{
    archive::{CatalogRoute, Preparation, ReaderCustody, WorkerBudgets},
    postgres::catalog_guard::CatalogIdentity,
};
use bigbrotr_protocol::archive_receipt::group::{GroupObservation, GroupRead, MAX_MEMBERS};
use serde::Serialize;

#[derive(Serialize)]
struct Input<'a> {
    catalog: super::super::bootstrap::Input<'a>,
    catalog_id: bigbrotr_model::OwnerResourceId,
    expected: GroupRead<'a>,
}

fn prepare_input(
    read: GroupRead<'_>,
    identity: &CatalogIdentity,
    route: &CatalogRoute<'_>,
    credential: CatalogReaderCredentials<'_>,
    allow_fd_growth: bool,
) -> Result<
    (
        zeroize::Zeroizing<String>,
        Option<super::super::client_identity::NativeTlsHandoff>,
    ),
    ReceiptError,
> {
    read.validate().map_err(|e| match e {
        bigbrotr_protocol::archive_receipt::ExpectationError::ResourceLimit => {
            ReceiptError::ResourceLimit
        }
        _ => ReceiptError::InvalidInput,
    })?;
    let (dsn, tls) = reader_connection(
        read.common.dataset_id,
        identity,
        route,
        credential,
        allow_fd_growth,
        "bigbrotr-archive-receipt-group",
    )?;
    let input = Input {
        catalog: super::super::bootstrap::Input {
            dsn: &dsn,
            data_root: identity.root(),
        },
        catalog_id: identity.catalog(),
        expected: read,
    };
    input
        .catalog
        .validate()
        .map_err(ReceiptError::Preparation)?;
    wire_fits(&input)?;
    Ok((dsn, tls))
}

impl CatalogRoute<'_> {
    /// Exact complete wire admission, including credential/route and FD growth,
    /// before any member lifetime is resolved. This performs no I/O.
    pub fn validate_receipt_group(
        &self,
        read: GroupRead<'_>,
        identity: &CatalogIdentity,
        credential: CatalogReaderCredentials<'_>,
    ) -> Result<(), ReceiptError> {
        prepare_input(read, identity, self, credential, true).map(|_| ())
    }
}

impl Preparation<'_> {
    /// The actual process retains reader custody and grants through full output,
    /// EOF and reap. Absence is not lifetime exclusion or publication authority.
    pub fn inspect_receipt_group_retained(
        self,
        read: GroupRead<'_>,
        custody: &ReaderCustody,
        route: &CatalogRoute<'_>,
        credential: CatalogReaderCredentials<'_>,
        budgets: WorkerBudgets<'_>,
    ) -> Result<Option<usize>, ReceiptError> {
        validate_receipt_limits(self.limits)?;
        if self.limits.max_records > MAX_MEMBERS || read.members.len() > self.limits.max_records {
            return Err(ReceiptError::ResourceLimit);
        }
        let identity = custody.identity();
        let (dsn, tls) = prepare_input(read, identity, route, credential, false)?;
        let input = Input {
            catalog: super::super::bootstrap::Input {
                dsn: &dsn,
                data_root: identity.root(),
            },
            catalog_id: identity.catalog(),
            expected: read,
        };
        let arguments = [self.limits.max_records.to_string()];
        let mut process = self
            .start_reader("inspect-receipt-group", &arguments, budgets, custody, tls)
            .map_err(ReceiptError::Preparation)?;
        let transfer = serde_json::to_writer(&mut process, &input);
        let output = process.finish();
        if matches!(output, Err(super::super::ProcessError::TerminationUnknown)) {
            return Err(ReceiptError::TerminationUnknown);
        }
        if transfer.is_err()
            || self.cancelled.load(std::sync::atomic::Ordering::Acquire)
            || std::time::Instant::now() >= self.deadline
        {
            return Err(ReceiptError::ObservationUnknown);
        }
        decode_response(output.map_err(|_| ReceiptError::ObservationUnknown)?, read)
    }
}

fn decode_response(output: &[u8], read: GroupRead<'_>) -> Result<Option<usize>, ReceiptError> {
    let response: GroupObservation =
        serde_json::from_slice(output).map_err(|_| ReceiptError::ObservationUnknown)?;
    response
        .validate(read)
        .map_err(|_| ReceiptError::ObservationUnknown)
}

#[cfg(test)]
mod tests;

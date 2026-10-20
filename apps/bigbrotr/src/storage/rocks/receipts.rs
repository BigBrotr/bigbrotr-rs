//! Bounded request-detail inspection. Absence cannot resolve an uncertain write.
use super::IngestStore;
use crate::{runtime::OwnedPermit, storage::rocks::keys::IngestKey};
use bigbrotr_model::{
    Digest256, ProducerRequestId, Receipt,
    receipt_codec::{ReceiptCodecError, ReceiptDecodeLimits},
};
use rocksdb::ReadOptions;
use std::{
    fmt,
    mem::size_of,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptReadError {
    InvalidLimits,
    InsufficientReservation,
    Cancelled,
    EngineRead,
    Decode(ReceiptCodecError),
    WrongRequest,
    ForeignDataset,
    RequestConflict,
}
impl fmt::Display for ReceiptReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request-detail read failed: {self:?}")
    }
}
impl std::error::Error for ReceiptReadError {}

/// Application allocation floor for the decoded vectors and returned value.
/// Native pins and I/O use the store's separate retained native allowance.
/// Allocator overhead and existing caller inputs are not included or an RSS cap.
pub fn required_receipt_data_bytes(limits: ReceiptDecodeLimits) -> Result<u64, ReceiptReadError> {
    if [
        limits.max_encoded_bytes,
        limits.max_allocation_bytes,
        limits.max_batch_records,
        limits.max_dependencies,
    ]
    .contains(&0)
    {
        return Err(ReceiptReadError::InvalidLimits);
    }
    limits
        .max_allocation_bytes
        .checked_add(size_of::<StoredReceipt>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or(ReceiptReadError::InvalidLimits)
}

/// Complete structural receipt retaining its reservation through the handoff.
/// It grants no current policy, producer, retention or dependency authority.
pub struct StoredReceipt {
    receipt: Receipt,
    native_outcome: Option<super::NativeOutcomeCopy>,
    _permit: OwnedPermit,
}
impl StoredReceipt {
    pub(super) fn bind_checkpoint(
        mut self,
        stamp: super::producer_checkpoint::CheckpointStamp,
    ) -> Self {
        // Only actual native mutation/replay callers own a copy. A structural
        // read deliberately remains unable to acquire checkpoint authority.
        if let Some(copy) = &mut self.native_outcome {
            copy.bind_checkpoint(stamp, None);
        }
        self
    }
    pub(super) fn new(
        receipt: Receipt,
        native_outcome: super::NativeOutcomeCopy,
        permit: OwnedPermit,
    ) -> Self {
        Self {
            receipt,
            native_outcome: Some(native_outcome),
            _permit: permit,
        }
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
    pub(in crate::storage) fn native_outcome(&self) -> Option<&super::NativeOutcomeCopy> {
        self.native_outcome.as_ref()
    }
}

impl IngestStore {
    /// Inspect the exact retained request result, preserving its original context.
    /// The owner must separately establish current authority, producer retirement,
    /// dependency closure and protection before acting on it. None means only
    /// absent detail, never rollback, RetiredRequest or permission to retry.
    /// Native reads cannot be cancelled mid-FFI; retain/join the worker. Cancellation
    /// is sampled before the read, before decoding and before returning a result.
    pub fn read_receipt(
        &self,
        request: ProducerRequestId,
        expected_digest: Digest256,
        limits: ReceiptDecodeLimits,
        permit: OwnedPermit,
        cancelled: &AtomicBool,
    ) -> Result<Option<StoredReceipt>, ReceiptReadError> {
        if permit.request().units().data_bytes() < required_receipt_data_bytes(limits)?
            || permit.request().units().items() == 0
        {
            return Err(ReceiptReadError::InsufficientReservation);
        }
        check_cancel(cancelled)?;
        let key = IngestKey::Request {
            request_id: request,
        }
        .encode()
        .map_err(|_| ReceiptReadError::WrongRequest)?;
        let cf = self
            .db
            .cf_handle("receipts")
            .ok_or(ReceiptReadError::EngineRead)?;
        let mut read = ReadOptions::default();
        read.set_verify_checksums(true);
        read.fill_cache(false);
        // One get is one committed value; no iterator or multi-key snapshot is
        // needed. Never copy a native value before checking its encoded bound.
        let value = self
            .db
            .get_pinned_cf_opt(cf, key.as_bytes(), &read)
            .map_err(|_| ReceiptReadError::EngineRead)?;
        check_cancel(cancelled)?;
        let Some(value) = value else { return Ok(None) };
        let receipt =
            Receipt::decode_persistent(&value, limits).map_err(ReceiptReadError::Decode)?;
        drop(value);
        if receipt.request_id() != request {
            return Err(ReceiptReadError::WrongRequest);
        }
        if receipt.protection_context().dataset_id != self.manifest.identity().dataset_id {
            return Err(ReceiptReadError::ForeignDataset);
        }
        if receipt.request_digest() != expected_digest {
            return Err(ReceiptReadError::RequestConflict);
        }
        check_cancel(cancelled)?;
        Ok(Some(StoredReceipt {
            receipt,
            native_outcome: None,
            _permit: permit,
        }))
    }
}

fn check_cancel(cancelled: &AtomicBool) -> Result<(), ReceiptReadError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(ReceiptReadError::Cancelled)
    } else {
        Ok(())
    }
}

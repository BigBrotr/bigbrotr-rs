//! Full-EOF observation of all exact alternatives, after parent lifetime fencing.
use super::{ReceiptError, scan_receipts};
use crate::{catalog::BootstrapRequest, engine::Engine};
use bigbrotr_model::OwnerResourceId;
use bigbrotr_protocol::archive_receipt::{
    MAX_INPUT_BYTES, ReceiptExpectation,
    group::{FORMAT, GroupObservation, GroupRead, MAX_MEMBERS, Member},
};
use duckdb::Connection;
use serde::Deserialize;
use std::io::Read;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    common: ReceiptExpectation,
    members: Vec<Member>,
}
impl Expected {
    fn borrowed(&self) -> GroupRead<'_> {
        GroupRead {
            common: &self.common,
            members: &self.members,
        }
    }
}

// No Debug/Serialize: the catalog request contains the private reader credential.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRequest {
    catalog: BootstrapRequest,
    catalog_id: OwnerResourceId,
    expected: Expected,
}
impl GroupRequest {
    pub fn read(input: impl Read) -> Result<Self, ReceiptError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MAX_INPUT_BYTES + 1)
            .map_err(|_| ReceiptError::ResourceLimit)?;
        input
            .take((MAX_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| ReceiptError::InvalidInput)?;
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(ReceiptError::ResourceLimit);
        }
        let request: Self =
            serde_json::from_slice(&bytes).map_err(|_| ReceiptError::InvalidInput)?;
        request
            .catalog
            .validate()
            .map_err(|_| ReceiptError::InvalidInput)?;
        request.expected.borrowed().validate()?;
        Ok(request)
    }
}

impl Engine {
    pub fn inspect_receipt_group(
        self,
        request: &GroupRequest,
        max_receipts: usize,
    ) -> Result<GroupObservation, ReceiptError> {
        let read = request.expected.borrowed();
        validate_scan(read, max_receipts)?;
        self.attach_readonly_catalog(
            &request.catalog,
            read.common.dataset_id,
            request.catalog_id,
            None,
        )?;
        let present = inspect_attached(&self.connection, read, max_receipts)?;
        Ok(GroupObservation {
            format: FORMAT.into(),
            request_digest: read.digest()?,
            present: present.map(|index| read.members[index]),
        })
        // Engine destruction closes native clients before response serialization.
    }
}

fn validate_scan(read: GroupRead<'_>, max_receipts: usize) -> Result<(), ReceiptError> {
    read.validate()?;
    if max_receipts == 0 || max_receipts > MAX_MEMBERS || read.members.len() > max_receipts {
        return Err(ReceiptError::ResourceLimit);
    }
    Ok(())
}

pub(super) fn inspect_attached(
    c: &Connection,
    read: GroupRead<'_>,
    max_receipts: usize,
) -> Result<Option<usize>, ReceiptError> {
    validate_scan(read, max_receipts)?;
    c.execute_batch("BEGIN TRANSACTION")
        .map_err(|_| ReceiptError::Native)?;
    let mut found = None;
    scan_receipts(c, read.common.dataset_id, max_receipts, |actual| {
        if actual.batch_digest == read.common.batch_digest
            || read.members.iter().any(|member| {
                member.attempt_id == actual.attempt_id
                    || member.publication_ordinal == actual.publication_ordinal
            })
        {
            let member = read.member_index(Member::from_receipt(&actual));
            if found.is_some() || !read.same_semantics(&actual) || member.is_none() {
                return Err(ReceiptError::Mismatch);
            }
            found = member;
        }
        Ok(())
    })?;
    c.execute_batch("COMMIT")
        .map_err(|_| ReceiptError::Native)?;
    Ok(found)
}

#[cfg(test)]
mod tests;

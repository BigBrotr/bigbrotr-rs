//! Replay exactly the persisted whole plan. Targets are serialized one at a time
//! so the account never needs overlapping data queries; zero outcomes still get
//! independent readback. No mutable row or caller-made plan can enter this path.
use super::{PhysicalError, PhysicalLimits, operation::QueryOwner};
use crate::storage::{
    clickhouse::{rows::*, schema::IndexTables},
    indexer::plan::PlannedBatch,
    postgres::{IndexProvenance, IndexTarget},
};
use bigbrotr_model::Fact;

pub(super) async fn batch(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    planned: &PlannedBatch,
    limits: PhysicalLimits,
) -> Result<(), PhysicalError> {
    let plan = planned.plan();
    tables
        .check_binding(plan.binding())
        .map_err(|_| PhysicalError::Schema)?;
    let provenance = plan.binding().provenance() == IndexProvenance::LocalFirstWitness;
    for target in IndexTarget::ALL {
        let expected = plan.chunks()[target as usize].row_count();
        let wire = planned.wire_bytes()[target as usize];
        if expected > limits.max_rows
            || wire
                .checked_add(super::MAX_QUERY_BYTES as u64 + 64 * 1024)
                .is_none_or(|n| n > limits.request_bytes)
        {
            return Err(PhysicalError::Limit);
        }
        if expected == 0 {
            if wire != 0 {
                return Err(PhysicalError::Source);
            }
            continue;
        }
        // Insert's internal DESCRIBE and the actual INSERT have different query
        // IDs. A completed metadata query cannot resolve an unknown data insert.
        macro_rules! insert {
            ($row:ty, $method:ident, $many:literal) => {{
                let metadata = owner.begin()?;
                let client = owner.client.clone().with_setting("query_id", &metadata);
                let insert = client
                    .insert::<$row>(&tables.qualified(target))
                    .await
                    .map_err(|error| owner.failed(&metadata, error))?;
                owner.completed(&metadata)?;
                let query = owner.begin()?;
                let mut insert = insert
                    .with_setting("query_id", &query)
                    .with_timeouts(Some(limits.query_timeout), Some(limits.query_timeout));
                let mut written = 0u64;
                for (bytes, reference) in planned
                    .source()
                    .records()
                    .iter()
                    .zip(planned.source().batch().records())
                {
                    let fact = Fact::decode_preimage(
                        bytes,
                        reference.payload_digest,
                        planned.fact_limits(),
                    )
                    .map_err(|_| PhysicalError::Source)?;
                    if fact.source_record_id() != reference.source_record_id {
                        return Err(PhysicalError::Source);
                    }
                    let mapped = FactRows::new(
                        plan.binding().generation(),
                        &fact,
                        provenance,
                        planned.fact_limits(),
                    )
                    .map_err(|_| PhysicalError::Rows)?;
                    // Option and the closed tag iterator both yield borrowed
                    // rows, without collecting a second payload representation.
                    for row in mapped.$method().into_iter() {
                        written = written
                            .checked_add(1)
                            .filter(|n| *n <= expected)
                            .ok_or(PhysicalError::Rows)?;
                        insert
                            .write(&row)
                            .await
                            .map_err(|error| owner.failed(&query, error))?;
                        tokio::task::yield_now().await;
                    }
                    tokio::task::yield_now().await;
                }
                if written != expected {
                    return Err(PhysicalError::Rows);
                }
                insert
                    .end()
                    .await
                    .map_err(|error| owner.failed(&query, error))?;
                owner.completed(&query)?;
            }};
        }
        match target {
            IndexTarget::Dictionaries => insert!(DictionaryRow<'static>, dictionary, false),
            IndexTarget::Events => insert!(EventRow<'static>, events, false),
            IndexTarget::EventEligibilityFacts => {
                insert!(EligibilityRow<'static>, event_eligibility_facts, false)
            }
            IndexTarget::EventReceiveFacts => insert!(ReceiveRow, event_receive_facts, false),
            IndexTarget::EventIdIndex
            | IndexTarget::EventAuthorIndex
            | IndexTarget::EventTimeIndex => insert!(EventIndexRow, event_index, false),
            IndexTarget::EventTagIndex => insert!(TagRow<'static>, event_tag_index, true),
            IndexTarget::ProvenanceByEvent | IndexTarget::ProvenanceByRelay => {
                insert!(ProvenanceRow, provenance, false)
            }
        }
    }
    Ok(())
}

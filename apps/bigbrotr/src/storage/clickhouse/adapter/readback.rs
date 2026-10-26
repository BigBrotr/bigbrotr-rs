//! Independent physical verification consumes complete distinct typed values.
//! Stored digest columns, raw counts, engine dedup tokens and merge timing are
//! never used as substitutes for hashing the actual returned row contents.
use super::{PhysicalError, PhysicalLimits, inspect, operation::QueryOwner};
use crate::storage::{
    clickhouse::{
        rows::*,
        schema::{self, IndexTables},
    },
    postgres::{IndexPlan, IndexTarget},
};
use bigbrotr_model::{Digest256, SourceCut};
use clickhouse::{Row, RowRead};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}

async fn family<R>(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    plan: &IndexPlan,
    target: IndexTarget,
    limits: PhysicalLimits,
) -> Result<ChunkSummary, PhysicalError>
where
    R: Row + RowRead,
    for<'a> R::Value<'a>: ProjectionRow,
{
    let chunk = &plan.chunks()[target as usize];
    if chunk.row_count() > limits.max_rows {
        return Err(PhysicalError::Limit);
    }
    let first = plan.source().first();
    let sql = format!(
        "SELECT DISTINCT {} FROM {} AS t WHERE t.projection_generation=unhex({{generation:String}}) AND t.source.1=unhex({{dataset:String}}) AND t.source.2={{shard:UInt32}} AND t.source.3={{epoch:UInt64}} AND t.source.4 BETWEEN {{first:UInt64}} AND {{last:UInt64}} ORDER BY t.source,t.discriminator LIMIT {}",
        schema::field_list(target),
        tables.qualified(target),
        chunk
            .row_count()
            .checked_add(1)
            .ok_or(PhysicalError::Limit)?
    );
    let id = owner.begin()?;
    let mut cursor = owner
        .client
        .query(&sql)
        .param("generation", hex(plan.binding().generation().as_bytes()))
        .param("dataset", hex(first.dataset_id.as_bytes()))
        .param("shard", first.shard_id)
        .param("epoch", first.owner_epoch.get())
        .param("first", first.sequence.get())
        .param("last", plan.source().last().sequence.get())
        .with_setting("query_id", &id)
        .fetch::<R>()
        .map_err(|error| owner.failed(&id, error))?;
    let mut hash = ChunkHasher::new(
        plan.binding().generation(),
        target,
        plan.after(),
        chunk.row_count(),
    );
    while let Some(row) = cursor
        .next()
        .await
        .map_err(|error| owner.failed(&id, error))?
    {
        hash.push(&row).map_err(|_| PhysicalError::Rows)?;
    }
    owner.completed(&id)?;
    let actual = hash.finish().map_err(|_| PhysicalError::Rows)?;
    if actual.row_count != chunk.row_count()
        || actual.rows_digest != chunk.rows_digest()
        || chunk.chunk_id() != chunk.rows_digest()
    {
        return Err(PhysicalError::Rows);
    }
    Ok(actual)
}

pub(super) async fn plan(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    plan: &IndexPlan,
    limits: PhysicalLimits,
) -> Result<Digest256, PhysicalError> {
    tables
        .check_binding(plan.binding())
        .map_err(|_| PhysicalError::Schema)?;
    inspect::installed(owner, tables, false).await?;
    let mut hash = receipt_start(tables.identity_digest(), plan.digest());
    for target in IndexTarget::ALL {
        let actual = match target {
            IndexTarget::Dictionaries => {
                family::<DictionaryRow<'static>>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::Events => {
                family::<EventRow<'static>>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::EventEligibilityFacts => {
                family::<EligibilityRow<'static>>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::EventReceiveFacts => {
                family::<ReceiveRow>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::EventIdIndex
            | IndexTarget::EventAuthorIndex
            | IndexTarget::EventTimeIndex => {
                family::<EventIndexRow>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::EventTagIndex => {
                family::<TagRow<'static>>(owner, tables, plan, target, limits).await?
            }
            IndexTarget::ProvenanceByEvent | IndexTarget::ProvenanceByRelay => {
                family::<ProvenanceRow>(owner, tables, plan, target, limits).await?
            }
        };
        receipt_row(&mut hash, target, actual.row_count, actual.rows_digest);
    }
    // A changed UUID/schema during the query sequence cannot yield a proof.
    inspect::installed(owner, tables, false).await?;
    Ok(Digest256::from_bytes(hash.finalize().into()))
}

#[derive(Row, Deserialize)]
struct CountRow {
    n: u64,
}
/// Every row belongs to the closed retained generation and contiguous complete
/// prefix being certified. Unknown streams/future rows are not hidden by filters.
/// Earlier per-batch checks establish all in-prefix memberships and zero outputs.
pub(super) async fn inside_cut(
    owner: &mut QueryOwner,
    tables: &IndexTables,
    generation: &[u8; 16],
    cut: &SourceCut,
) -> Result<(), PhysicalError> {
    let mut streams = String::new();
    for (i, bound) in cut.bounds().iter().enumerate() {
        if i != 0 {
            streams.push_str(" OR ");
        }
        streams.push_str(&format!(
            "(t.source.2={} AND t.source.3={} AND t.source.4<={})",
            bound.stream.shard_id,
            bound.stream.owner_epoch.get(),
            bound.last_complete_sequence.get()
        ));
    }
    if streams.is_empty() {
        return Err(PhysicalError::Source);
    }
    for target in IndexTarget::ALL {
        let sql = format!(
            "SELECT count() AS n FROM {} AS t WHERE t.projection_generation!=unhex({{generation:String}}) OR t.source.1!=unhex({{dataset:String}}) OR t.source.4=0 OR NOT({streams})",
            tables.qualified(target)
        );
        if sql.len() > super::MAX_QUERY_BYTES {
            return Err(PhysicalError::Limit);
        }
        let query = owner
            .client
            .query(&sql)
            .param("generation", hex(generation))
            .param("dataset", hex(cut.dataset_id().as_bytes()));
        let rows = inspect::collect::<CountRow>(owner, query, 1).await?;
        if rows.len() != 1 || rows[0].n != 0 {
            return Err(PhysicalError::Rows);
        }
    }
    Ok(())
}

fn receipt_start(table_digest: Digest256, plan_digest: Digest256) -> Sha256 {
    let mut hash = Sha256::new();
    hash.update(b"bigbrotr/index-engine-receipt/1\0");
    hash.update(table_digest.as_bytes());
    hash.update(plan_digest.as_bytes());
    hash
}
fn receipt_row(hash: &mut Sha256, target: IndexTarget, count: u64, rows: Digest256) {
    hash.update([target as u8]);
    hash.update(count.to_be_bytes());
    hash.update(rows.as_bytes());
}

#[cfg(test)]
#[path = "readback/tests.rs"]
mod tests;

//! Fixed projections and serial writes under actual Control/dataset custody.
//! All dynamic epoch/topology values come from the checked portable record.
use super::*;
use bigbrotr_model::{DictionaryKind, OwnerEpoch, OwnerResourceId, ProcessIncarnation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Intent {
    pub(super) record: SuccessionRecord,
    pub(super) role: [u8; 16],
    pub(super) policy: PolicySnapshot,
    pub(super) created: i64,
    pub(super) deadline: i64,
    pub(super) limit: u32,
    pub(super) preparer: ingest_registry::ControlOwner,
}
impl Intent {
    pub(super) fn epoch(&self) -> u64 {
        self.record
            .fields()
            .next_manifest
            .identity()
            .owner_epoch
            .get()
    }
}

pub(super) async fn clock(tx: &Transaction<'_>) -> Result<i64, ProducerResolveError> {
    tx.query_one(
        "SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint",
        &[],
    )
    .await
    .map_err(backend)?
    .try_get(0)
    .map_err(|e| backend(e).into())
}
pub(super) async fn current_policy(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
) -> Result<PolicySnapshot, ProducerResolveError> {
    let row = tx.query_one("SELECT left(d.config_revision::text,21),left(d.policy_revision::text,21),
        substring(c.document_digest FROM 1 FOR 33),substring(p.document_digest FROM 1 FOR 33)
        FROM control.datasets d JOIN control.config_revisions c ON c.dataset_id=d.dataset_id AND c.revision=d.config_revision
        JOIN control.policy_revisions p ON p.dataset_id=d.dataset_id AND p.revision=d.policy_revision AND p.config_revision=c.revision
        WHERE d.dataset_id=$1::bytea", &[&&recovery.manifest().identity().dataset_id.as_bytes()[..]])
        .await.map_err(backend)?;
    policy(&row, 0)
}
pub(super) async fn require_policy(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    expected: PolicySnapshot,
) -> Result<(), ProducerResolveError> {
    if current_policy(tx, recovery).await? != expected {
        return Err(OwnerError::InvalidPolicy.into());
    }
    Ok(())
}

pub(super) async fn read(
    tx: &Transaction<'_>,
    recovery: &IngestRecovery,
    epoch: u64,
) -> Result<Intent, ProducerResolveError> {
    read_at(tx, recovery.manifest().identity().owner_resource_id, epoch).await
}
async fn read_at(
    tx: &Transaction<'_>,
    resource: OwnerResourceId,
    epoch: u64,
) -> Result<Intent, ProducerResolveError> {
    if !(3..=255).contains(&epoch) {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let row = tx.query_one("SELECT substring(s.handoff FROM 1 FOR 644),substring(s.target_role_instance_id FROM 1 FOR 17),
        left(s.config_revision::text,21),left(s.policy_revision::text,21),
        substring(s.config_digest FROM 1 FOR 33),substring(s.policy_digest FROM 1 FOR 33),
        s.created_at_us,s.lease_expires_at_us,s.outstanding_request_limit::bigint,
        substring(s.prepared_by_resource FROM 1 FOR 17),left(s.prepared_by_epoch::text,21),
        substring(o.role_instance_id FROM 1 FOR 17),substring(s.prepared_by_incarnation FROM 1 FOR 17)
        FROM control.ingest_successions s JOIN control.owner_epochs o
            ON o.resource_id=s.prepared_by_resource AND o.epoch=s.prepared_by_epoch
        WHERE s.owner_resource_id=$1::bytea AND s.target_epoch=$2::text::numeric",
        &[&&resource.as_bytes()[..], &epoch.to_string()]).await.map_err(backend)?;
    let record = SuccessionRecord::decode(row.try_get(0).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata)?;
    if record.fields().next_manifest.identity().owner_resource_id != resource
        || record.fields().next_manifest.identity().owner_epoch.get() != epoch
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(Intent {
        record,
        role: fixed(&row, 1)?,
        policy: policy(&row, 2)?,
        created: row.try_get(6).map_err(backend)?,
        deadline: row.try_get(7).map_err(backend)?,
        limit: u32::try_from(row.try_get::<_, i64>(8).map_err(backend)?)
            .map_err(|_| OwnerError::InvalidMetadata)?,
        preparer: ingest_registry::ControlOwner {
            resource_id: OwnerResourceId::try_from(fixed::<16>(&row, 9)?)
                .map_err(|_| OwnerError::InvalidMetadata)?,
            epoch: OwnerEpoch::new(decimal(&row, 10)?).map_err(|_| OwnerError::InvalidMetadata)?,
            role_instance_id: fixed(&row, 11)?,
            process_incarnation: ProcessIncarnation::try_from(fixed::<16>(&row, 12)?)
                .map_err(|_| OwnerError::InvalidMetadata)?,
        },
    })
}
pub(super) async fn require_intent(
    tx: &Transaction<'_>,
    intent: &Intent,
) -> Result<(), ProducerResolveError> {
    if read_at(
        tx,
        intent
            .record
            .fields()
            .next_manifest
            .identity()
            .owner_resource_id,
        intent.epoch(),
    )
    .await?
        != *intent
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}
pub(super) async fn require_target(
    tx: &Transaction<'_>,
    intent: &Intent,
    purpose: &str,
) -> Result<Option<i64>, ProducerResolveError> {
    tx.query_one(
        "SELECT control.require_ingest_successor($1::bytea,$2::text::numeric,$3::text)",
        &[
            &&intent
                .record
                .fields()
                .next_manifest
                .identity()
                .owner_resource_id
                .as_bytes()[..],
            &intent.epoch().to_string(),
            &purpose,
        ],
    )
    .await
    .map_err(backend)?
    .try_get(0)
    .map_err(|e| backend(e).into())
}
pub(super) async fn require_empty_claims(
    tx: &Transaction<'_>,
    intent: &Intent,
) -> Result<(), ProducerResolveError> {
    tx.query_one(
        "SELECT control.require_empty_ingest_claims($1::bytea,$2::text::numeric)",
        &[
            &&intent
                .record
                .fields()
                .next_manifest
                .identity()
                .owner_resource_id
                .as_bytes()[..],
            &intent.epoch().to_string(),
        ],
    )
    .await
    .map_err(backend)?;
    Ok(())
}
pub(super) async fn require_empty_digest(
    tx: &Transaction<'_>,
    intent: &Intent,
    digest: [u8; 32],
) -> Result<(), ProducerResolveError> {
    let exact: bool = tx.query_one("SELECT control.ingest_lineage_closure_digest($1::bytea,$2::text::numeric,true)=$3::bytea", &[
        &&intent.record.fields().next_manifest.identity().owner_resource_id.as_bytes()[..], &intent.epoch().to_string(), &&digest[..],
    ]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !exact {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(())
}

pub(super) async fn lock_control(
    tx: &Transaction<'_>,
    current: ingest_registry::ControlOwner,
) -> Result<(), ProducerResolveError> {
    tx.query_one("SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric FOR UPDATE",
        &[&&current.resource_id.as_bytes()[..], &current.epoch.get().to_string()]).await.map_err(backend)?;
    Ok(())
}
pub(super) async fn require_live(
    tx: &Transaction<'_>,
    current: ingest_registry::ControlOwner,
    target: Option<&Intent>,
) -> Result<(), ProducerResolveError> {
    let row = tx.query_one("SELECT control.require_control_owner(dataset_id,resource_id,epoch,process_incarnation),
        lease_expires_at_us>(extract(epoch FROM clock_timestamp())*1000000)::bigint
        FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric AND process_incarnation=$3::bytea", &[
        &&current.resource_id.as_bytes()[..], &current.epoch.get().to_string(), &&current.process_incarnation.as_bytes()[..],
    ]).await.map_err(backend)?;
    if !row.try_get::<_, bool>(1).map_err(backend)? {
        return Err(OwnerError::Unavailable.into());
    }
    if let Some(intent) = target {
        if intent.deadline <= clock(tx).await? {
            return Err(OwnerError::Unavailable.into());
        }
    }
    Ok(())
}

pub(super) async fn require_fresh(
    tx: &Transaction<'_>,
    intent: &Intent,
) -> Result<(), ProducerResolveError> {
    let f = intent.record.fields();
    let id = f.next_manifest.identity();
    if intent.role == [0; 16]
        || intent.role == intent.preparer.role_instance_id
        || intent.role == *intent.preparer.process_incarnation.as_bytes()
        || intent.role == *id.process_incarnation.as_bytes()
        || id.owner_resource_id == intent.preparer.resource_id
        || intent.policy.policy_revision != id.policy_epoch.get()
        || !(1..=64).contains(&intent.limit)
        || intent.deadline <= intent.created
    {
        return Err(OwnerError::InvalidInput.into());
    }
    let fresh: bool = tx.query_one("SELECT NOT EXISTS(SELECT 1 FROM control.role_instances WHERE role_instance_id=$1::bytea)
        AND NOT EXISTS(SELECT 1 FROM control.owner_epochs WHERE dataset_id=$2::bytea AND process_incarnation=$3::bytea)
        AND NOT EXISTS(SELECT 1 FROM control.ingest_successions WHERE owner_resource_id=$4::bytea AND target_epoch=$5::text::numeric)", &[
        &&intent.role[..], &&id.dataset_id.as_bytes()[..], &&id.process_incarnation.as_bytes()[..],
        &&id.owner_resource_id.as_bytes()[..], &intent.epoch().to_string(),
    ]).await.map_err(backend)?.try_get(0).map_err(backend)?;
    if !fresh {
        return Err(OwnerError::InvalidMetadata.into());
    }
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).expect("assigned dictionary kind");
        let domain = f
            .next_manifest
            .allocation_namespaces()
            .domain(kind)
            .ok_or(OwnerError::InvalidMetadata)?;
        let present: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM control.allocation_domains WHERE dataset_id=$1::bytea AND allocation_domain=$2::bytea)",
            &[&&id.dataset_id.as_bytes()[..], &&domain.as_bytes()[..]]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if present {
            return Err(OwnerError::InvalidMetadata.into());
        }
    }
    require_live(tx, intent.preparer, Some(intent)).await
}

pub(super) async fn insert(
    tx: &Transaction<'_>,
    intent: &Intent,
) -> Result<(), ProducerResolveError> {
    let f = intent.record.fields();
    let id = f.next_manifest.identity();
    let dataset = &id.dataset_id.as_bytes().as_slice();
    let resource = &id.owner_resource_id.as_bytes().as_slice();
    let role = &intent.role.as_slice();
    let incarnation = &id.process_incarnation.as_bytes().as_slice();
    let epoch = intent.epoch().to_string();
    let previous_epoch = f.predecessor_epoch.get().to_string();
    let previous = f
        .previous_manifest
        .identity()
        .topology_generation
        .get()
        .to_string();
    let target = id.topology_generation.get().to_string();
    let shard = i64::from(id.shard_id);
    tx.execute("INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us) VALUES($1::bytea,$2::bytea,'ingest',$3::bigint)", &[role,dataset,&intent.created]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state) VALUES($1::bytea,$2::text::numeric,$3::bytea,'ingest',$4::bytea,$5::bytea,$6::bigint,$7::bigint,'active')", &[resource,&epoch,dataset,role,incarnation,&intent.created,&intent.deadline]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.topology_generations(dataset_id,generation,previous_generation,created_at_us) VALUES($1::bytea,$2::text::numeric,$3::text::numeric,$4::bigint)", &[dataset,&target,&previous,&intent.created]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.source_streams(dataset_id,shard_id,owner_epoch,owner_resource_id,owner_incarnation,opened_at_us) VALUES($1::bytea,$2::bigint,$3::text::numeric,$4::bytea,$5::bytea,$6::bigint)", &[dataset,&shard,&epoch,resource,incarnation,&intent.created]).await.map_err(backend)?;
    // The flat predecessor checker already proved the exact bounded source set.
    // Stream its immutable cuts inside SQL; do not allocate another PG Vec.
    let copied = tx.execute("INSERT INTO control.topology_streams(dataset_id,topology_generation,shard_id,owner_epoch,handoff_last_complete_sequence)
        SELECT dataset_id,$2::text::numeric,shard_id,owner_epoch,last_complete_sequence FROM control.source_streams
        WHERE dataset_id=$1::bytea AND owner_resource_id=$3::bytea AND owner_epoch<$4::text::numeric AND state='closed'
        ORDER BY owner_epoch LIMIT 256", &[dataset,&target,resource,&epoch]).await.map_err(backend)?;
    if copied != f.predecessor_epoch.get() {
        return Err(OwnerError::InvalidMetadata.into());
    }
    tx.execute("INSERT INTO control.topology_streams(dataset_id,topology_generation,shard_id,owner_epoch) VALUES($1::bytea,$2::text::numeric,$3::bigint,$4::text::numeric)", &[dataset,&target,&shard,&epoch]).await.map_err(backend)?;
    for number in 1..=9 {
        let kind = DictionaryKind::try_from(number).expect("assigned dictionary kind");
        let domain = f
            .next_manifest
            .allocation_namespaces()
            .domain(kind)
            .ok_or(OwnerError::InvalidMetadata)?;
        tx.execute("INSERT INTO control.allocation_domains(dataset_id,dictionary_kind,allocation_domain,owner_resource_id,owner_epoch,owner_incarnation,created_at_us) VALUES($1::bytea,$2::smallint,$3::bytea,$4::bytea,$5::text::numeric,$6::bytea,$7::bigint)", &[dataset,&(kind as i16),&domain.as_bytes().as_slice(),resource,&epoch,incarnation,&intent.created]).await.map_err(backend)?;
    }
    tx.execute("INSERT INTO control.producer_epochs(producer_slot_id,producer_epoch,dataset_id,producer_role_instance_id,producer_role,producer_incarnation,owner_resource_id,owner_epoch,owner_incarnation,outstanding_request_limit,opened_at_us) VALUES($1::bytea,$2::text::numeric,$3::bytea,$4::bytea,'ingest',$5::bytea,$6::bytea,$2::text::numeric,$5::bytea,$7::bigint,$8::bigint)", &[&&f.producer_slot_id.as_bytes()[..],&epoch,dataset,role,incarnation,resource,&i64::from(intent.limit),&intent.created]).await.map_err(backend)?;
    tx.execute("UPDATE control.topology_generations SET sealed=true WHERE dataset_id=$1::bytea AND generation=$2::text::numeric", &[dataset,&target]).await.map_err(backend)?;
    if tx.execute("UPDATE control.datasets SET topology_generation=$2::text::numeric WHERE dataset_id=$1::bytea AND topology_generation=$3::text::numeric", &[dataset,&target,&previous]).await.map_err(backend)? != 1 {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let old = f.previous_manifest.encode();
    let next = f.next_manifest.encode();
    let handoff = intent.record.encode();
    tx.execute("INSERT INTO control.ingest_successions(dataset_id,owner_resource_id,target_epoch,previous_epoch,terminal_kind,
        target_incarnation,target_role_instance_id,producer_slot_id,producer_epoch,shard_id,previous_topology,target_topology,
        issued_through,last_complete_sequence,closure_digest,previous_manifest,target_manifest,previous_manifest_sha256,target_manifest_sha256,
        handoff,handoff_sha256,parent_handoff_sha256,config_revision,policy_revision,config_digest,policy_digest,outstanding_request_limit,
        created_at_us,lease_expires_at_us,prepared_by_resource,prepared_by_epoch,prepared_by_incarnation)
        VALUES($1::bytea,$2::bytea,$3::text::numeric,$4::text::numeric,$5::smallint,$6::bytea,$7::bytea,$8::bytea,$3::text::numeric,
        $9::bigint,$10::text::numeric,$11::text::numeric,$12::text::numeric,$13::text::numeric,$14::bytea,$15::bytea,$16::bytea,
        sha256($15::bytea),sha256($16::bytea),$17::bytea,sha256($17::bytea),$18::bytea,$19::text::numeric,$20::text::numeric,
        $21::bytea,$22::bytea,$23::bigint,$24::bigint,$25::bigint,$26::bytea,$27::text::numeric,$28::bytea)", &[
        dataset,resource,&epoch,&previous_epoch,&(f.terminal_kind as i16),incarnation,role,&&f.producer_slot_id.as_bytes()[..],
        &shard,&previous,&target,&f.issued_through.to_string(),&f.last_complete_sequence.to_string(),&&f.closure_digest[..],
        &old.as_bytes(),&next.as_bytes(),&handoff.as_bytes(),&&f.incoming_handoff_sha256[..],
        &intent.policy.config_revision.to_string(),&intent.policy.policy_revision.to_string(),&&intent.policy.config_digest[..],&&intent.policy.policy_digest[..],
        &i64::from(intent.limit),&intent.created,&intent.deadline,&&intent.preparer.resource_id.as_bytes()[..],
        &intent.preparer.epoch.get().to_string(),&&intent.preparer.process_incarnation.as_bytes()[..],
    ]).await.map_err(backend)?;
    Ok(())
}

pub(super) async fn retire_empty(
    tx: &Transaction<'_>,
    intent: &Intent,
    digest: [u8; 32],
) -> Result<i64, ProducerResolveError> {
    require_empty_digest(tx, intent, digest).await?;
    let id = intent.record.fields().next_manifest.identity();
    let dataset = &id.dataset_id.as_bytes().as_slice();
    let resource = &id.owner_resource_id.as_bytes().as_slice();
    let slot = &intent
        .record
        .fields()
        .producer_slot_id
        .as_bytes()
        .as_slice();
    let epoch = intent.epoch().to_string();
    let time = clock(tx).await?;
    if time < intent.created {
        return Err(OwnerError::InvalidMetadata.into());
    }
    let source_closing = tx.execute("UPDATE control.source_streams SET state='closing' WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=$3::text::numeric AND state='open'", &[dataset,resource,&epoch]).await.map_err(backend)?;
    let source_closed = tx.execute("UPDATE control.source_streams SET state='closed',closed_at_us=$4::bigint,last_complete_sequence=0,closure_digest=$5::bytea WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=$3::text::numeric AND state='closing'", &[dataset,resource,&epoch,&time,&&digest[..]]).await.map_err(backend)?;
    let producer_closing = tx.execute("UPDATE control.producer_epochs SET state='closing' WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric AND state='open'", &[slot,&epoch]).await.map_err(backend)?;
    let producer_retired = tx.execute("UPDATE control.producer_epochs SET state='retired',retired_at_us=$3::bigint,closure_digest=$4::bytea WHERE producer_slot_id=$1::bytea AND producer_epoch=$2::text::numeric AND state='closing'", &[slot,&epoch,&time,&&digest[..]]).await.map_err(backend)?;
    let domains = tx.execute("UPDATE control.allocation_domains SET state='closed',closed_at_us=$4::bigint,closure_digest=$5::bytea WHERE dataset_id=$1::bytea AND owner_resource_id=$2::bytea AND owner_epoch=$3::text::numeric AND state='active'", &[dataset,resource,&epoch,&time,&&digest[..]]).await.map_err(backend)?;
    let owner = tx.execute("UPDATE control.owner_epochs SET state='released',released_at_us=$3::bigint,exclusion_method='storage_fenced',exclusion_digest=$4::bytea WHERE resource_id=$1::bytea AND epoch=$2::text::numeric AND state='active'", &[resource,&epoch,&time,&&digest[..]]).await.map_err(backend)?;
    let role = tx.execute("UPDATE control.role_instances SET retired_at_us=$2::bigint WHERE role_instance_id=$1::bytea AND retired_at_us IS NULL", &[&&intent.role[..],&time]).await.map_err(backend)?;
    if [
        source_closing,
        source_closed,
        producer_closing,
        producer_retired,
        owner,
        role,
    ] != [1; 6]
        || domains != 9
    {
        return Err(OwnerError::InvalidMetadata.into());
    }
    Ok(time)
}

fn fixed<const N: usize>(row: &Row, column: usize) -> Result<[u8; N], ProducerResolveError> {
    row.try_get::<_, &[u8]>(column)
        .map_err(backend)?
        .try_into()
        .map_err(|_| OwnerError::InvalidMetadata.into())
}
fn decimal(row: &Row, column: usize) -> Result<u64, ProducerResolveError> {
    ingest_registry::decimal(row.try_get(column).map_err(backend)?)
        .map_err(|_| OwnerError::InvalidMetadata.into())
}
fn policy(row: &Row, column: usize) -> Result<PolicySnapshot, ProducerResolveError> {
    Ok(PolicySnapshot {
        config_revision: decimal(row, column)?,
        policy_revision: decimal(row, column + 1)?,
        config_digest: fixed(row, column + 2)?,
        policy_digest: fixed(row, column + 3)?,
    })
}

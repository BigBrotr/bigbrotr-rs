//! One fenced scope-grant mutation. Receipts never authorize data delivery.
use super::{
    ArchivePinReceipt, ControlSession,
    browser_reauthentication::{authenticated_session, require_manager},
    browser_sessions::*,
    require,
};
use crate::{
    management::{
        credentials::random,
        grants::{GrantCommand, GrantReceipt, Revision},
        sessions::{BrowserSecret, SessionError},
    },
    storage::{
        archive::ArchivedVisibility,
        postgres::{catalog_guard::CatalogCapture, initialization::application_transaction},
    },
};
use bigbrotr_model::{CommandKey, DatasetId, ReauthenticationId};
use std::cell::Cell;
use tokio_postgres::Transaction;

/// Real native evidence and its retained barriers. All identities are compared
/// again inside the accepting transaction; these references alone grant nothing.
/// The caller owns and joins both transports and native work before releasing
/// local custody or the pin, including an unknown COMMIT.
pub struct ScopeGrantAllocation<'a, 'capture> {
    pub capture: &'a CatalogCapture<'capture>,
    pub visibility: &'a ArchivedVisibility,
    pub pin: &'a ArchivePinReceipt,
}
impl ControlSession {
    /// Check the command's explicit schema and application privileges before
    /// exposing its route. This neither reads native data nor grants an action.
    pub async fn require_scope_grants(&mut self) -> Result<(), SessionError> {
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 811)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let valid: bool = tx.query_one(
            "SELECT bool_and(has_table_privilege(CURRENT_USER,t.name,p.privilege)) AND has_column_privilege(CURRENT_USER,'control.reauthentication_proofs','proof_id','UPDATE') AND has_function_privilege(CURRENT_USER,'control.command_u64_bytes(control.u64)','EXECUTE') AND has_function_privilege(CURRENT_USER,'control.scope_pin_live(bytea,bytea,numeric,bytea)','EXECUTE') FROM (VALUES ('control.management_commands',ARRAY['SELECT','INSERT']),('control.scope_grant_changes',ARRAY['SELECT','INSERT']),('control.scope_grants',ARRAY['SELECT','INSERT','UPDATE'])) t(name,privileges) CROSS JOIN LATERAL unnest(t.privileges) p(privilege)", &[])
            .await.map_err(backend)?.try_get(0).map_err(metadata)?;
        if !valid {
            return Err(SessionError::Prerequisite);
        }
        tx.rollback().await.map_err(backend)?;
        self.available = true;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn accept_scope_grant(
        &mut self,
        token: &BrowserSecret,
        csrf: &BrowserSecret,
        key: CommandKey,
        command: &GrantCommand,
        proof: ReauthenticationId,
        allocation: Option<&ScopeGrantAllocation<'_, '_>>,
        commit_started: &Cell<bool>,
    ) -> Result<GrantReceipt, SessionError> {
        command.validate(self.request.dataset)?;
        self.begin_operation()
            .map_err(|_| SessionError::Unavailable)?;
        let tx = application_transaction(&mut self.client, 811)
            .await
            .map_err(|_| SessionError::Prerequisite)?;
        require(&tx, self.request.dataset, self.identity)
            .await
            .map_err(|_| SessionError::Unavailable)?;
        lock_dataset(&tx, self.request.dataset, self.validated_policy).await?;
        let outcome = accept_in_transaction(
            &tx,
            self.request.dataset,
            self.validated_policy,
            token,
            csrf,
            key,
            command,
            proof,
            allocation,
        )
        .await;

        let outcome = match outcome {
            // No Control error occurs in these native/capacity prerequisites.
            // A known rollback preserves the otherwise healthy Control session.
            Err(SessionError::ArchiveUnavailable) => Err(SessionError::ArchiveUnavailable),
            other => terminal(&tx, other)?,
        };
        match outcome {
            Ok(receipt) if receipt.created => {
                commit_started.set(true);
                if let Err(error) = tx.commit().await {
                    if let Some(rejection) = error.as_db_error() {
                        if rejection.code().code() == "BB001" {
                            // The server explicitly rolled back on expiry. No
                            // ambiguity or lost-response retry is implied.
                            self.available = true;
                            return Err(SessionError::Denied);
                        }
                        return Err(SessionError::Backend);
                    }
                    return Err(SessionError::CommitUnknown);
                }
                self.available = true;
                Ok(receipt)
            }
            outcome => {
                // Expected refusal after any provisional writes rolls back the
                // entire acceptance, including audit/command; never commit half.
                tx.rollback().await.map_err(backend)?;
                self.available = true;
                outcome
            }
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn accept_in_transaction(
    tx: &Transaction<'_>,
    dataset_id: DatasetId,
    policy: super::PolicySnapshot,
    token: &BrowserSecret,
    csrf: &BrowserSecret,
    key: CommandKey,
    command: &GrantCommand,
    proof: ReauthenticationId,
    allocation: Option<&ScopeGrantAllocation<'_, '_>>,
) -> Result<GrantReceipt, SessionError> {
    let current = authenticated_session(
        tx,
        dataset_id,
        &token.session_digest(),
        &csrf.csrf_digest(),
        Some(command.principal_id),
    )
    .await?;
    let digest = command.digest(dataset_id, current.principal, key)?;
    // The actor comes from this live authenticated session, never a
    // caller-supplied principal. Old acceptance is resolved before proof,
    // current permission, CAS and Archive dependencies, with no writes.
    if let Some(row)=tx.query_opt("SELECT c.dataset_id=$3::bytea AND c.action='scope-grant.change' AND c.request_digest=$4::bytea,left(d.scope_generation::text,21),left(d.resulting_grant_revision::text,21),c.accepted_at_us FROM control.management_commands c LEFT JOIN control.scope_grant_changes d USING(actor_id,request_key) WHERE c.actor_id=$1::bytea AND c.request_key=$2::bytea", &[&&current.principal.as_bytes()[..],&&key.as_bytes()[..],&&dataset_id.as_bytes()[..],&&digest.as_bytes()[..]]).await.map_err(backend)? {
                if !row.try_get::<_,bool>(0).map_err(metadata)? {return Err(SessionError::Conflict);}
                return receipt(command,key,revision(&row,1)?,revision(&row,2)?,row.try_get(3).map_err(metadata)?,false);
            }
    require_manager(tx, dataset_id, current.principal).await?;
    if command.expected_config_revision.get() != policy.config_revision
        || command.expected_policy_revision.get() != policy.policy_revision
    {
        return Err(SessionError::Conflict);
    }
    let target = lock_account(tx, dataset_id, command.principal_id).await?;
    if !target.try_get::<_, bool>(0).map_err(metadata)? {
        return Err(SessionError::Denied);
    }
    if revision(&target, 3)? != command.expected_grant_revision.get() {
        return Err(SessionError::Conflict);
    }
    let next_account = command
        .expected_grant_revision
        .get()
        .checked_add(1)
        .ok_or(SessionError::Capacity)?;
    let row=tx.query_opt("SELECT enabled,left(generation::text,21) FROM control.scope_grants WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='history.read' AND dictionary_kind=4 AND allocation_domain=$3::bytea AND ordinal=$4::text::numeric", &[&&dataset_id.as_bytes()[..],&&command.principal_id.as_bytes()[..],&&command.scope.allocation_domain.as_bytes()[..],&command.scope.ordinal.get().to_string()]).await.map_err(backend)?;
    let generation = match row {
        Some(row) => {
            if row.try_get::<_, bool>(0).map_err(metadata)? == command.enabled {
                return Err(SessionError::Conflict);
            }
            revision(&row, 1)?
                .checked_add(1)
                .ok_or(SessionError::Capacity)?
        }
        None if command.enabled => 1,
        None => return Err(SessionError::Conflict),
    };
    let proof_row=tx.query_opt("SELECT dataset_id=$2::bytea AND actor_id=$3::bytea AND session_id=$4::bytea AND request_key=$5::bytea AND request_digest=$6::bytea AND config_revision=$7::text::numeric AND policy_revision=$8::text::numeric AND credential_revision=$9::text::numeric AND authentication_revision=$10::text::numeric AND grant_revision=$11::text::numeric,issued_at_us,expires_at_us FROM control.reauthentication_proofs WHERE proof_id=$1::bytea FOR UPDATE", &[&&proof.as_bytes()[..],&&dataset_id.as_bytes()[..],&&current.principal.as_bytes()[..],&&current.id[..],&&key.as_bytes()[..],&&digest.as_bytes()[..],&policy.config_revision.to_string(),&policy.policy_revision.to_string(),&revision(&current.account,1)?.to_string(),&revision(&current.account,2)?.to_string(),&revision(&current.account,3)?.to_string()]).await.map_err(backend)?.ok_or(SessionError::Denied)?;
    let started = now(tx).await?;
    let proof_expires: i64 = proof_row.try_get(2).map_err(metadata)?;
    if !proof_row.try_get::<_, bool>(0).map_err(metadata)?
        || started < proof_row.try_get::<_, i64>(1).map_err(metadata)?
        || started >= proof_expires
        || started >= current.expires
    {
        return Err(SessionError::Denied);
    }
    if command.enabled {
        allocation
            .ok_or(SessionError::ArchiveUnavailable)?
            .verify(tx, dataset_id, command)
            .await?;
    } else if allocation.is_some() {
        return Err(SessionError::InvalidInput);
    }
    let time = now(tx).await?;
    let audit = random::<16>().map_err(|_| SessionError::Unavailable)?;
    let dataset = &dataset_id.as_bytes()[..];
    let actor = &current.principal.as_bytes()[..];
    let request_key = &key.as_bytes()[..];
    tx.execute("INSERT INTO control.management_audit(audit_id,dataset_id,actor_id,action,outcome,created_at_us,config_revision,subject_id,policy_revision) VALUES($1::bytea,$2::bytea,$3::bytea,'scope-grant.change','accepted',$4::bigint,$5::text::numeric,$6::bytea,$7::text::numeric)", &[&&audit[..],&dataset,&actor,&time,&command.expected_config_revision.get().to_string(),&request_key,&command.expected_policy_revision.get().to_string()]).await.map_err(backend)?;
    tx.execute("INSERT INTO control.management_commands(actor_id,request_key,dataset_id,action,request_digest,expected_config_revision,policy_revision,audit_id,accepted_at_us) VALUES($1::bytea,$2::bytea,$3::bytea,'scope-grant.change',$4::bytea,$5::text::numeric,$6::text::numeric,$7::bytea,$8::bigint)", &[&actor,&request_key,&dataset,&&digest.as_bytes()[..],&command.expected_config_revision.get().to_string(),&command.expected_policy_revision.get().to_string(),&&audit[..],&time]).await.map_err(backend)?;
    let catalog = command
        .allocation
        .as_ref()
        .map(|a| &a.catalog_id.as_bytes()[..]);
    let frontier = command
        .allocation
        .as_ref()
        .map(|a| a.frontier_revision.get().to_string());
    let pin = allocation.map(|a| &a.pin.lease_id[..]);
    let changed=tx.execute("INSERT INTO control.scope_grant_changes(actor_id,request_key,dataset_id,principal_id,action,dictionary_kind,allocation_domain,ordinal,enabled,scope_generation,expected_grant_revision,resulting_grant_revision,proof_id,session_id,actor_credential_revision,actor_authentication_revision,actor_grant_revision,proof_expires_at_us,catalog_id,frontier_revision,pin_lease_id) VALUES($1::bytea,$2::bytea,$3::bytea,$4::bytea,'history.read',4,$5::bytea,$6::text::numeric,$7::boolean,$8::text::numeric,$9::text::numeric,$10::text::numeric,$11::bytea,$12::bytea,$13::text::numeric,$14::text::numeric,$15::text::numeric,$16::bigint,$17::bytea,$18::text::numeric,$19::bytea)", &[&actor,&request_key,&dataset,&&command.principal_id.as_bytes()[..],&&command.scope.allocation_domain.as_bytes()[..],&command.scope.ordinal.get().to_string(),&command.enabled,&generation.to_string(),&command.expected_grant_revision.get().to_string(),&next_account.to_string(),&&proof.as_bytes()[..],&&current.id[..],&revision(&current.account,1)?.to_string(),&revision(&current.account,2)?.to_string(),&revision(&current.account,3)?.to_string(),&proof_expires,&catalog,&frontier,&pin]).await.map_err(backend)?;
    if changed == 0 {
        return Err(SessionError::Denied);
    }
    one(changed)?;
    one(tx.execute("UPDATE control.local_accounts SET grant_revision=$3::text::numeric WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND grant_revision=$4::text::numeric", &[&dataset,&&command.principal_id.as_bytes()[..],&next_account.to_string(),&command.expected_grant_revision.get().to_string()]).await.map_err(backend)?)?;
    if generation == 1 {
        one(tx.execute("INSERT INTO control.scope_grants SELECT dataset_id,principal_id,action,dictionary_kind,allocation_domain,ordinal,enabled,scope_generation,actor_id,request_key FROM control.scope_grant_changes WHERE actor_id=$1::bytea AND request_key=$2::bytea", &[&actor,&request_key]).await.map_err(backend)?)?;
    } else {
        one(tx.execute("UPDATE control.scope_grants SET enabled=$5::boolean,generation=$6::text::numeric,actor_id=$7::bytea,request_key=$8::bytea WHERE dataset_id=$1::bytea AND principal_id=$2::bytea AND action='history.read' AND dictionary_kind=4 AND allocation_domain=$3::bytea AND ordinal=$4::text::numeric", &[&dataset,&&command.principal_id.as_bytes()[..],&&command.scope.allocation_domain.as_bytes()[..],&command.scope.ordinal.get().to_string(),&command.enabled,&generation.to_string(),&actor,&request_key]).await.map_err(backend)?)?;
    }
    one(tx
        .execute(
            "DELETE FROM control.reauthentication_proofs WHERE proof_id=$1::bytea",
            &[&&proof.as_bytes()[..]],
        )
        .await
        .map_err(backend)?)?;
    if let Some(a) = allocation {
        a.capture
            .verify()
            .await
            .map_err(|_| SessionError::ArchiveUnavailable)?;
    }
    receipt(command, key, generation, next_account, time, true)
}

fn one(changed: u64) -> Result<(), SessionError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(SessionError::InvalidMetadata)
    }
}
fn receipt(
    command: &GrantCommand,
    key: CommandKey,
    generation: u64,
    account: u64,
    time: i64,
    created: bool,
) -> Result<GrantReceipt, SessionError> {
    Ok(GrantReceipt {
        command_key: key,
        principal_id: command.principal_id,
        scope: command.scope,
        enabled: command.enabled,
        scope_generation: Revision::try_from(generation.to_string())?,
        account_grant_revision: Revision::try_from(account.to_string())?,
        accepted_at_us: time,
        created,
    })
}
impl ScopeGrantAllocation<'_, '_> {
    async fn verify(
        &self,
        tx: &Transaction<'_>,
        dataset: DatasetId,
        command: &GrantCommand,
    ) -> Result<(), SessionError> {
        let selected = command
            .allocation
            .as_ref()
            .ok_or(SessionError::InvalidInput)?;
        let read = self.visibility.read();
        if self.capture.identity() != self.visibility.catalog()
            || self.capture.identity().dataset() != dataset
            || self.capture.identity().catalog() != selected.catalog_id
            || self.pin.catalog_id != selected.catalog_id
            || self.pin.frontier_revision != selected.frontier_revision.get()
            || self.pin.released
            || self.pin.snapshot_id != self.capture.snapshot_id()
            || read.dataset != dataset
            || read.snapshot_id != self.pin.snapshot_id
            || read.scope != command.scope
            || !self.capture.coordinator().permits_reader_pins()
        {
            return Err(SessionError::Conflict);
        }
        self.capture
            .verify()
            .await
            .map_err(|_| SessionError::ArchiveUnavailable)?;
        let valid: bool = tx
            .query_one(
                "SELECT control.scope_pin_live($1::bytea,$2::bytea,$3::text::numeric,$4::bytea)",
                &[
                    &&dataset.as_bytes()[..],
                    &&selected.catalog_id.as_bytes()[..],
                    &selected.frontier_revision.get().to_string(),
                    &&self.pin.lease_id[..],
                ],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(metadata)?;
        if !valid {
            return Err(SessionError::Conflict);
        }
        let row=tx.query_one("SELECT f.snapshot_id=$4::bigint AND f.topology_generation=$5::text::numeric AND p.coordinator_binding_sha256=$6::bytea FROM control.archive_frontier_snapshots f JOIN control.archive_persistence p ON p.dataset_id=f.dataset_id AND p.catalog_id=f.catalog_id WHERE f.dataset_id=$1::bytea AND f.catalog_id=$2::bytea AND f.frontier_revision=$3::text::numeric", &[&&dataset.as_bytes()[..],&&selected.catalog_id.as_bytes()[..],&selected.frontier_revision.get().to_string(),&(read.snapshot_id as i64),&read.topology_generation.get().to_string(),&&self.capture.coordinator().binding_digest().as_bytes()[..]]).await.map_err(backend)?;
        if !row.try_get::<_, bool>(0).map_err(metadata)? {
            return Err(SessionError::Conflict);
        }
        let bounds=tx.query("SELECT shard_id::bigint,left(owner_epoch::text,21),left(last_complete_sequence::text,21) FROM control.consumer_frontiers WHERE consumer_id=$1::bytea AND frontier_revision=$2::text::numeric ORDER BY shard_id,owner_epoch LIMIT 1025", &[&&selected.catalog_id.as_bytes()[..],&selected.frontier_revision.get().to_string()]).await.map_err(backend)?;
        if bounds.len() > 1024 {
            return Err(SessionError::Capacity);
        }
        if bounds.len() != read.bounds.len() {
            return Err(SessionError::Conflict);
        }
        for (row, bound) in bounds.iter().zip(&read.bounds) {
            let last: &str = row.try_get(2).map_err(metadata)?;
            let last = last
                .parse::<u64>()
                .map_err(|_| SessionError::InvalidMetadata)?;
            if row.try_get::<_, i64>(0).map_err(metadata)? != i64::from(bound.shard_id)
                || revision(row, 1)? != bound.owner_epoch
                || last != bound.complete_through
            {
                return Err(SessionError::Conflict);
            }
        }
        self.capture
            .verify()
            .await
            .map_err(|_| SessionError::ArchiveUnavailable)
    }
}

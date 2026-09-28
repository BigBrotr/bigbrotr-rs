//! Same-lifetime Ingest scheduling renewal; never native writer authority.
use super::*;
use crate::runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl};
use bigbrotr_model::PolicyEpoch;

impl ControlSession {
    /// Renew only this Control owner's complete current initial registration.
    /// The Ingest deadline becomes the existing Control scheduling deadline;
    /// refresh Control first. Neither expired owner can be revived here.
    ///
    /// The caller retains its native owner independently. Success grants no
    /// native access or policy lease, and changes no incarnation, producer state,
    /// fact, request or receipt. Fresh native operations still reauthorize.
    /// Reserve both complete policy documents, impose the caller's I/O deadline,
    /// and retain/join the driver after cancellation. Any failure after starting
    /// coordination poisons this session. A lost COMMIT can have renewed the row;
    /// resolve it as scheduling data, never impersonate this owner on restart.
    pub async fn renew_initial_ingest(
        &mut self,
        spec: &ingest_registry::InitialRegistration,
        policy_ttl: Duration,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
        commit_started: &Cell<bool>,
    ) -> Result<(), OwnerError> {
        if spec.dataset_id != self.request.dataset
            || spec.instance_id != *self.request.instance.as_bytes()
            || spec.control != self.identity
        {
            return Err(OwnerError::InvalidInput);
        }
        let epoch = PolicyEpoch::new(spec.policy_revision);
        let mut lease = PolicyLeaseTracker::new(
            spec.dataset_id,
            spec.process_incarnation,
            epoch,
            PolicyLeaseTtl::new(policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 14)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy.config_revision != spec.config_revision
            || policy.policy_revision != spec.policy_revision
            || policy.config_digest != *spec.config_digest.as_bytes()
            || policy.policy_digest != *spec.policy_digest.as_bytes()
        {
            return Err(OwnerError::InvalidPolicy);
        }
        // Control -> dataset -> Ingest -> producer. Acquire the write lock
        // before the registry's shared dependent locks, not by late promotion.
        tx.query_opt(
            "SELECT epoch FROM control.owner_epochs
              WHERE resource_id = $1::bytea AND epoch = 1 FOR UPDATE",
            &[&&spec.ingest_resource_id.as_bytes()[..]],
        )
        .await
        .map_err(backend)?
        .ok_or(OwnerError::InvalidMetadata)?;
        ingest_registry::require_initial_registration(&tx, spec)
            .await
            .map_err(|error| match error {
                ingest_registry::RegistryError::Backend => OwnerError::Backend,
                ingest_registry::RegistryError::InvalidInput => OwnerError::InvalidInput,
                ingest_registry::RegistryError::HeadsChanged => OwnerError::InvalidPolicy,
                _ => OwnerError::InvalidMetadata,
            })?;
        const REMAINING: &str = "
            SELECT LEAST(c.lease_expires_at_us, i.lease_expires_at_us)
                   - (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint
              FROM control.owner_epochs c CROSS JOIN control.owner_epochs i
             WHERE c.resource_id = $1::bytea AND c.epoch = $2::text::numeric
               AND i.resource_id = $3::bytea AND i.epoch = 1";
        let control = &spec.control.resource_id.as_bytes().as_slice();
        let control_epoch = spec.control.epoch.get().to_string();
        let ingest = &spec.ingest_resource_id.as_bytes().as_slice();
        let remaining: i64 = tx
            .query_one(REMAINING, &[control, &control_epoch, ingest])
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(OwnerError::Unavailable)?;
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: spec.dataset_id,
                    process_incarnation: spec.process_incarnation,
                    policy_epoch: epoch,
                    ttl: policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        // Recheck the OLD deadlines in the write statement after every wait.
        // A scheduling extension must not revive a row that expired meanwhile.
        const RENEW: &str = "
            WITH observed AS MATERIALIZED (
                SELECT (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp()) * 1000000)::bigint AS now_us
            )
            UPDATE control.owner_epochs i
               SET lease_expires_at_us = c.lease_expires_at_us
              FROM control.owner_epochs c, observed
             WHERE c.resource_id = $1::bytea AND c.epoch = $2::text::numeric
               AND i.resource_id = $3::bytea AND i.epoch = 1
               AND c.lease_expires_at_us > observed.now_us
               AND i.lease_expires_at_us > observed.now_us";
        if tx
            .execute(RENEW, &[control, &control_epoch, ingest])
            .await
            .map_err(backend)?
            != 1
        {
            return Err(OwnerError::Unavailable);
        }
        lease
            .check_delivery(epoch)
            .map_err(|_| OwnerError::Unavailable)?;
        commit_started.set(true);
        tx.commit().await.map_err(|_| OwnerError::CommitUnknown)?;
        if self.client.is_closed() || lease.check_delivery(epoch).is_err() {
            return Err(OwnerError::Unavailable);
        }
        self.available = true;
        Ok(())
    }
}

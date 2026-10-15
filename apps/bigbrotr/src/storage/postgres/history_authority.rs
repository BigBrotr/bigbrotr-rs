//! Shared current authority, after a protocol-specific historical identity check.
//! This private kernel does not classify a native schema or grant a lifecycle.
use super::*;
use crate::{
    runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    storage::rocks::store::IngestRecovery,
};
use bigbrotr_model::PolicyEpoch;

impl ControlSession {
    pub(super) async fn begin_history_authority<'a, E>(
        &'a mut self,
        recovery: &IngestRecovery,
        input: &InitialHistoryRequest<'_>,
        required_schema: u32,
        mut validate: impl FnMut(&str, [u8; 32]) -> Result<(), OwnerError>,
    ) -> Result<(Transaction<'a>, PolicyLeaseTracker, PolicyEpoch), ProducerOperationError<E>> {
        let identity = recovery.manifest().identity();
        let mut lease = PolicyLeaseTracker::new(
            self.request.dataset,
            self.identity.process_incarnation,
            identity.policy_epoch,
            PolicyLeaseTtl::new(input.policy_ttl).map_err(|_| OwnerError::InvalidInput)?,
        )
        .map_err(|_| OwnerError::Unavailable)?;
        let ticket = lease.begin_refresh().map_err(|_| OwnerError::Unavailable)?;
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, required_schema)
            .await
            .map_err(transaction_error)?;
        if required_schema >= 31 {
            // Generic zero-request import claims take SHARE on this Control.
            // Exclude them before dataset/target custody and any absence check.
            tx.query_one(
                "SELECT epoch FROM control.owner_epochs WHERE resource_id=$1::bytea
                 AND epoch=$2::text::numeric FOR UPDATE",
                &[
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?;
        }
        require(&tx, self.request.dataset, self.identity).await?;
        // snapshot takes dataset UPDATE before historical parent/producer locks.
        let policy = snapshot(&tx, &self.request, &mut validate).await?;
        if policy.policy_revision < identity.policy_epoch.get() {
            return Err(OwnerError::InvalidPolicy.into());
        }
        let remaining: i64 = tx
            .query_one(
                "SELECT lease_expires_at_us-(EXTRACT(EPOCH FROM clock_timestamp())*1000000)::bigint
             FROM control.owner_epochs WHERE resource_id=$1::bytea AND epoch=$2::text::numeric",
                &[
                    &&self.identity.resource_id.as_bytes()[..],
                    &self.identity.epoch.get().to_string(),
                ],
            )
            .await
            .map_err(backend)?
            .try_get(0)
            .map_err(backend)?;
        let remaining = u64::try_from(remaining)
            .ok()
            .filter(|n| *n > 0)
            .ok_or(OwnerError::Unavailable)?;
        let epoch = PolicyEpoch::new(policy.policy_revision);
        lease
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset: self.request.dataset,
                    process_incarnation: self.identity.process_incarnation,
                    policy_epoch: epoch,
                    ttl: input.policy_ttl.min(Duration::from_micros(remaining)),
                },
            )
            .map_err(|_| OwnerError::Unavailable)?;
        initial_history::check_delivery(&mut lease, epoch, input.cancelled)?;
        Ok((tx, lease, epoch))
    }
}

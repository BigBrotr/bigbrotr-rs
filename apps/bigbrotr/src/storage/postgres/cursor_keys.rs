//! Bind this Control incarnation's loaded bytes to immutable enrollment history.
use super::{
    ControlSession, OwnerError, application_transaction, backend, fixed, require, transaction_error,
};
use crate::read::keyring::Keyring;
use std::sync::Arc;

impl ControlSession {
    /// Observe permanent enrollment only, not current issuance/decryption authority
    /// or the presence of every lifecycle-required key. The transport supervisor
    /// retains another Arc through the actual driver join, including cancellation.
    /// No reload or implicit provisioning occurs on an existing session.
    pub async fn bind_cursor_keys(&mut self, keys: Arc<Keyring>) -> Result<(), OwnerError> {
        if keys.dataset() != self.request.dataset || self.enrolled_cursor_keys.is_some() {
            return Err(OwnerError::InvalidInput);
        }
        self.begin_operation()?;
        let tx = application_transaction(&mut self.client, 602)
            .await
            .map_err(transaction_error)?;
        require(&tx, self.request.dataset, self.identity).await?;
        let rows = tx.query("SELECT substring(dataset_id FROM 1 FOR 17), substring(instance_id FROM 1 FOR 17) FROM control.datasets LIMIT 2 FOR SHARE", &[]).await.map_err(backend)?;
        let [row] = rows.as_slice() else {
            return Err(OwnerError::IncompatibleDeployment);
        };
        if fixed::<16>(row, 0)? != *self.request.dataset.as_bytes()
            || fixed::<16>(row, 1)? != *self.request.instance.as_bytes()
        {
            return Err(OwnerError::IncompatibleDeployment);
        }
        let dataset = &self.request.dataset.as_bytes()[..];
        let row = tx.query_opt("SELECT substring(keyring_id FROM 1 FOR 17), file_format FROM control.cursor_keyrings WHERE dataset_id=$1::bytea", &[&dataset]).await.map_err(backend)?
            .ok_or(OwnerError::IncompatibleDeployment)?;
        if fixed::<16>(&row, 0)? != keys.id() || row.try_get::<_, i32>(1).map_err(backend)? != 1 {
            return Err(OwnerError::IncompatibleDeployment);
        }
        // At most16 indexed point lookups. Never scan historical bindings or
        // compare against the original whole-file digest/order/count in0601.
        for (id, digest) in keys.bindings() {
            let row = tx.query_opt("SELECT substring(keyring_id FROM 1 FOR 17), substring(material_digest FROM 1 FOR 33) FROM control.cursor_key_materials WHERE dataset_id=$1::bytea AND key_id=$2::bytea", &[&dataset, &&id[..]]).await.map_err(backend)?
                .ok_or(OwnerError::IncompatibleDeployment)?;
            if fixed::<16>(&row, 0)? != keys.id() || fixed::<32>(&row, 1)? != *digest.as_bytes() {
                return Err(OwnerError::IncompatibleDeployment);
            }
        }
        // require_control_owner retains the backend fence and row locks, but
        // does not check lease time. Waits above must not admit an expired owner.
        let live: bool = tx.query_one("SELECT lease_expires_at_us > (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp())*1000000)::bigint FROM control.owner_epochs WHERE dataset_id=$1::bytea AND resource_id=$2::bytea AND epoch=$3::text::numeric AND process_incarnation=$4::bytea AND role_instance_id=$5::bytea AND state='active'", &[&dataset, &&self.identity.resource_id.as_bytes()[..], &self.identity.epoch.get().to_string(), &&self.identity.process_incarnation.as_bytes()[..], &&self.identity.role_instance_id[..]]).await.map_err(backend)?.try_get(0).map_err(backend)?;
        if !live {
            return Err(OwnerError::Unavailable);
        }
        tx.rollback().await.map_err(backend)?;
        self.enrolled_cursor_keys = Some(keys);
        self.available = true;
        Ok(())
    }

    /// Non-authorizing local snapshot; future cursor actions must independently
    /// check current lifecycle, policy and principal. A poisoned owner exposes none.
    pub fn enrolled_cursor_keys(&self) -> Option<&Keyring> {
        self.available.then_some(())?;
        self.enrolled_cursor_keys.as_deref()
    }
}

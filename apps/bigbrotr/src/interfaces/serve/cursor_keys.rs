//! Startup-only local loading; SQL enrollment is checked by the acquired owner.
use super::{Resolved, Result};
use bigbrotr::{
    read::keyring::{KEYRING_RESERVATION, Keyring},
    runtime::{ResourceBudget, ResourceRequest},
};
use bigbrotr_model::DatasetId;
use std::sync::Arc;

pub(super) fn load(
    root: &Resolved,
    dataset: DatasetId,
    budget: &ResourceBudget,
) -> Result<Option<Arc<Keyring>>> {
    root.text("read.cursor.keyring_ref")
        .map(|reference| {
            let permit = budget
                .try_acquire(
                    ResourceRequest::data(KEYRING_RESERVATION, 1, 0)
                        .expect("fixed keyring reservation"),
                )
                .map_err(|_| {
                    (
                        3,
                        "cursor keyring does not fit the shared process budget".into(),
                    )
                })?;
            Keyring::open(reference, dataset, permit)
                .map(Arc::new)
                .map_err(|error| (3, error.to_string()))
        })
        .transpose()
}

pub(super) fn observation(keys: Option<&Keyring>) -> serde_json::Value {
    use serde_json::json;
    match keys {
        Some(keys) => json!({"state":"enrolled-local-snapshot", "dataset_id":keys.dataset(),
            "keyring_id":format!("{:032x}",u128::from_be_bytes(keys.id())),
            "key_ids":keys.key_ids().map(|id| format!("{:032x}",u128::from_be_bytes(id))).collect::<Vec<_>>(),
            "cursor_authority":"not-granted"}),
        None => json!({"state":"not-selected", "cursor_authority":"not-granted"}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigbrotr::runtime::ResourceLimits;
    use std::{fs, os::unix::fs::PermissionsExt};

    #[test]
    fn loaded_snapshot_uses_shared_capacity_and_retains_it_until_the_last_owner() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = temporary.path().canonicalize().unwrap();
        let config = directory.join("root.toml");
        let original = include_str!("../../../../../config/recipes/archive-only.toml");
        fs::write(&config, original).unwrap();
        let plain = crate::config::load(&config).unwrap();
        let unselected_budget = super::super::management::budget(&plain).unwrap();
        let dataset = "22222222222222222222222222222222".parse().unwrap();
        assert!(load(&plain, dataset, &unselected_budget).unwrap().is_none());
        assert_eq!(unselected_budget.snapshot().active_permits(), 0);
        let keys = directory.join("keys");
        fs::create_dir(&keys).unwrap();
        fs::set_permissions(&keys, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            &config,
            format!(
                "{original}\n[read.cursor]\nkeyring_ref='file:{}/ring'\n",
                keys.display()
            ),
        )
        .unwrap();
        let selected = crate::config::load(&config).unwrap();
        let budget = super::super::management::budget(&selected).unwrap();
        assert_eq!(
            budget.snapshot().limits().items(),
            unselected_budget.snapshot().limits().items() + 1
        );
        assert_eq!(
            budget.snapshot().limits().data_bytes(),
            unselected_budget.snapshot().limits().data_bytes()
        );
        budget
            .set_limits(ResourceLimits::new(KEYRING_RESERVATION - 1, 1, 0, 0))
            .unwrap();
        let error = load(&selected, dataset, &budget).unwrap_err();
        assert!(error.1.contains("shared process budget"));
        assert_eq!(budget.snapshot().active_permits(), 0);
        budget
            .set_limits(ResourceLimits::new(KEYRING_RESERVATION, 1, 0, 0))
            .unwrap();
        let reference = selected.text("read.cursor.keyring_ref").unwrap();
        let prepared = Keyring::prepare(
            reference,
            dataset,
            budget
                .try_acquire(ResourceRequest::data(KEYRING_RESERVATION, 1, 0).unwrap())
                .unwrap(),
        )
        .unwrap();
        let ring = prepared.id();
        drop(prepared);
        let snapshot = load(&selected, dataset, &budget).unwrap().unwrap();
        let transport_custody = Arc::clone(&snapshot);
        assert_eq!(budget.snapshot().used().data_bytes(), KEYRING_RESERVATION);
        assert_eq!(budget.snapshot().used().items(), 1);
        assert!(load(&selected, dataset, &budget).is_err());
        // A pathname change cannot change an already loaded snapshot.
        fs::write(keys.join("ring"), b"deliberately corrupted local fixture").unwrap();
        assert_eq!(snapshot.id(), ring);
        drop(snapshot);
        assert_eq!(budget.snapshot().active_permits(), 1);
        drop(transport_custody);
        assert_eq!(budget.snapshot().active_permits(), 0);
        assert!(load(&selected, dataset, &budget).is_err());
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

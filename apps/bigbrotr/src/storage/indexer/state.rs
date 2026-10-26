//! Retained local input to explicit installation and verified Index recovery.
//! A readable manifest never substitutes for the authoritative PG/CH lineage.
use super::custody::{CustodyError, HeldIndexCustody};
use crate::storage::{
    clickhouse::schema::{IndexTables, SchemaError},
    postgres::{IndexBinding, IndexProvenance},
};
use bigbrotr_model::{DatasetId, OwnerResourceId, ProcessIncarnation, ProjectionGenerationId};
use std::path::Path;

const _: () = assert!(
    super::custody::MAX_TABLE_MANIFEST_BYTES
        == crate::storage::clickhouse::schema::MAX_TABLE_MANIFEST_BYTES
);

#[derive(Debug)]
pub enum StateError {
    Custody(CustodyError),
    Schema(SchemaError),
    Binding,
}
impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "retained Index state refused: {self:?}")
    }
}
impl std::error::Error for StateError {}

/// Only local descriptor custody and validated configuration. Construction does
/// not grant publication, certify remote absence or resume a partial install.
pub struct IndexState {
    tables: IndexTables,
    custody: HeldIndexCustody,
}
impl IndexState {
    /// Persist the complete fresh identities before any remote installation.
    /// A failure leaves its partial journal intact for explicit inspection.
    pub fn create(
        root: &Path,
        dataset: DatasetId,
        resource: OwnerResourceId,
        generation: ProjectionGenerationId,
        storage_incarnation: ProcessIncarnation,
        tables: IndexTables,
        provenance: IndexProvenance,
    ) -> Result<Self, StateError> {
        let bytes = tables.encode_manifest().map_err(StateError::Schema)?;
        let custody = HeldIndexCustody::create(
            root,
            dataset,
            resource,
            generation,
            storage_incarnation,
            tables.identity_digest(),
            provenance,
        )
        .map_err(StateError::Custody)?;
        custody
            .create_table_manifest(&bytes)
            .map_err(StateError::Custody)?;
        Ok(Self { tables, custody })
    }

    /// Reacquire the same local store and exact table identity. The service must
    /// still require the retained PG generation and full physical verification;
    /// missing PG state is not permission to adopt or initialize an old journal.
    pub fn recover(root: &Path) -> Result<Self, StateError> {
        let custody = HeldIndexCustody::recover_stored(root).map_err(StateError::Custody)?;
        Self::from_custody(custody)
    }

    fn from_custody(custody: HeldIndexCustody) -> Result<Self, StateError> {
        let bytes = custody.read_table_manifest().map_err(StateError::Custody)?;
        let tables = IndexTables::decode_manifest(&bytes).map_err(StateError::Schema)?;
        if tables.identity_digest() != custody.binding().table_identity_digest() {
            return Err(StateError::Binding);
        }
        Ok(Self { tables, custody })
    }

    pub fn binding(&self) -> &IndexBinding {
        self.custody.binding()
    }
    pub fn tables(&self) -> &IndexTables {
        &self.tables
    }
    pub fn custody(&self) -> &HeldIndexCustody {
        &self.custody
    }
}

#[cfg(test)]
mod tests {
    use super::super::custody::tests::{BOOT, create_bound, recover_released, root};
    use super::*;
    use std::fs;

    #[test]
    fn recovered_tables_require_the_retained_binding_and_preserve_rejected_bytes() {
        let (_directory, path) = root();
        let tables = IndexTables::new(
            "index_fixture".into(),
            std::array::from_fn(|i| [i as u8 + 1; 16]),
        )
        .unwrap();
        let bytes = tables.encode_manifest().unwrap();
        let custody = create_bound(&path, tables.identity_digest()).unwrap();
        let binding = *custody.binding();
        custody.create_table_manifest(&bytes).unwrap();
        drop(custody);
        // Real descriptors/flock and the real decoder; only host/boot selection
        // uses the existing portable filesystem fixture, not remote authority.
        let recovered = IndexState::recover_for_test(&path, &binding).unwrap();
        assert_eq!(recovered.tables(), &tables);
        assert_eq!(recovered.binding(), &binding);
        recovered.custody().check_live().unwrap();
        drop(recovered);

        let manifest = path.join(".bigbrotr-index-custody/tables.json");
        let other = IndexTables::new(
            "other_database".into(),
            std::array::from_fn(|i| [i as u8 + 1; 16]),
        )
        .unwrap()
        .encode_manifest()
        .unwrap();
        fs::write(&manifest, &other).unwrap();
        assert!(matches!(
            IndexState::recover_for_test(&path, &binding),
            Err(StateError::Binding)
        ));
        assert_eq!(fs::read(&manifest).unwrap(), other);

        let mut invalid: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        invalid["undeclared"] = serde_json::json!(true);
        let invalid = serde_json::to_vec(&invalid).unwrap();
        fs::write(&manifest, &invalid).unwrap();
        assert!(matches!(
            IndexState::recover_for_test(&path, &binding),
            Err(StateError::Schema(_))
        ));
        assert_eq!(fs::read(&manifest).unwrap(), invalid);
        fs::remove_file(&manifest).unwrap();
        assert!(matches!(
            IndexState::recover_for_test(&path, &binding),
            Err(StateError::Custody(CustodyError::Incomplete))
        ));
        assert!(!manifest.exists());
    }

    impl IndexState {
        fn recover_for_test(path: &Path, binding: &IndexBinding) -> Result<Self, StateError> {
            let custody = recover_released(path, binding, BOOT).map_err(StateError::Custody)?;
            Self::from_custody(custody)
        }
    }
}

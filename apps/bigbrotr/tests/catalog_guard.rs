use bigbrotr::storage::postgres::catalog_guard::{CatalogIdentity, GuardError};
use bigbrotr_model::{DatasetId, OwnerResourceId};

#[test]
fn catalog_identity_requires_an_explicit_bounded_canonical_root() {
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let catalog = OwnerResourceId::try_from([2; 16]).unwrap();
    for invalid in [
        "",
        "relative",
        "/archive",
        "/archive/../other/",
        "/archive//",
        "/archive/./",
        "/archive/\0/",
        "s3://bucket/archive/",
    ] {
        assert_eq!(
            CatalogIdentity::local(dataset, catalog, invalid).unwrap_err(),
            GuardError::InvalidInput
        );
    }
    let value = CatalogIdentity::local(dataset, catalog, "/archive/it's owned/").unwrap();
    assert_eq!(value.root(), "/archive/it's owned/");
    assert_eq!(value.dataset(), dataset);
    assert_eq!(value.catalog(), catalog);
    assert_eq!(
        CatalogIdentity::local(dataset, catalog, &format!("/{}/", "a".repeat(4096))).unwrap_err(),
        GuardError::InvalidInput
    );
}

#[test]
fn scientific_identity_cannot_equal_native_only_foundation_identity() {
    let dataset = DatasetId::try_from([1; 16]).unwrap();
    let catalog = OwnerResourceId::try_from([2; 16]).unwrap();
    assert_ne!(
        CatalogIdentity::local(dataset, catalog, "/archive/").unwrap(),
        CatalogIdentity::native_only_local(dataset, catalog, "/archive/").unwrap()
    );
}

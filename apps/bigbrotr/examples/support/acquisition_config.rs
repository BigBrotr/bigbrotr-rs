//! Compile the actual configuration owners into the finite engine probe. This
//! omits only CLI dispatch; no policy parser or source decision is reimplemented.
#[path = "../../src/config/acquisition.rs"]
pub(crate) mod acquisition;
#[path = "../../src/config/policy.rs"]
pub(crate) mod policy;
#[path = "../../src/config/private_routes.rs"]
mod private_routes;
#[path = "../../src/config/recipes.rs"]
mod recipes;
#[path = "../../src/config/registry.rs"]
mod registry;
#[path = "../../src/config/resolve.rs"]
pub(crate) mod resolve;
pub(crate) use resolve::Resolved;

pub(crate) fn fixture_root(dataset: bigbrotr_model::DatasetId, instance: [u8; 16]) -> Resolved {
    let deployment = include_str!("../../../../config/recipes/archive-only.toml")
        .replace(
            "22222222222222222222222222222222",
            &crate::hex(dataset.as_bytes()),
        )
        .replace("11111111111111111111111111111111", &crate::hex(&instance));
    resolve::parse(&deployment).unwrap()
}

pub(crate) fn validate_fixture_policy(
    document: &str,
    digest: [u8; 32],
    root: &Resolved,
) -> Result<(), bigbrotr::storage::postgres::control_owner::OwnerError> {
    policy::PolicySeed::restore(document, digest, root.clone())
        .map(|_| ())
        .map_err(|_| bigbrotr::storage::postgres::control_owner::OwnerError::InvalidPolicy)
}

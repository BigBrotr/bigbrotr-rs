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

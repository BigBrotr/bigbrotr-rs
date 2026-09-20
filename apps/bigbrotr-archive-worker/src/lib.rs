//! Private native boundary. No DuckDB object crosses the process boundary.
pub mod bundle;
pub mod catalog;
pub mod engine;
pub mod prepare;
pub mod publication;
pub mod receipt;
pub mod rows;
mod schema;

pub mod initialization_custody;

pub mod event;
pub mod membership;

pub mod cut;
pub mod visibility;

#[cfg(test)]
#[path = "visibility/tests.rs"]
mod visibility_tests;

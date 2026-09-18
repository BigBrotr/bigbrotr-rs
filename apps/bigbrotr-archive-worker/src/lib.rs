//! Private native boundary. No DuckDB object crosses the process boundary.
pub mod bundle;
pub mod catalog;
pub mod engine;
pub mod prepare;
pub mod publication;
pub mod rows;
mod schema;

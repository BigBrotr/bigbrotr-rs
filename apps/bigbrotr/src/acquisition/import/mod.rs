//! Source capture and positioned JSONL verification, before job/admission authority.
mod jsonl;
mod source;

pub use jsonl::{JsonlReader, JsonlRecord, RecordPosition};
pub use source::{ImportSourceError, ScratchSpace, SourceIdentity, SourceSnapshot};

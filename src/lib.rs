//! Data types and logic for the Echo liquid handler.
//!
//! This crate holds protocol-neutral data — picklists, labware definitions,
//! plate survey results — that a user can author or export without talking to
//! the instrument. Instrument communication lives in the sibling `kithairon-link`
//! crate.

pub mod labware;
pub use labware::{Labware, PlateInfo};

#[cfg(feature = "python")]
mod python;

/// Errors raised by the `kithairon` crate.
#[derive(Debug, thiserror::Error)]
pub enum LibraryError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("XML deserialization failed: {0}")]
    XmlDeserialize(#[from] quick_xml::DeError),

    #[error(
        "XML could not be parsed as either ELWX or ELW.\n  ELWX error: {elwx}\n  ELW error: {elw}"
    )]
    ParseBothFormats { elwx: String, elw: String },

    #[error("a plate with type {0:?} already exists in this labware")]
    DuplicatePlateType(String),
}

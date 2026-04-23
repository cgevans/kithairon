//! Data types and logic for the Echo liquid handler.
//!
//! This crate holds protocol-neutral data — picklists, labware definitions,
//! plate survey results — that a user can author or export without talking to
//! the instrument. Instrument communication lives in the sibling `kithairon-link`
//! crate.

#[cfg(feature = "python")]
mod python;

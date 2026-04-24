//! Data types and logic for the Echo liquid handler.
//!
//! This crate holds protocol-neutral data — picklists, labware definitions,
//! plate survey results — that a user can author or export without talking to
//! the instrument. Instrument communication lives in the sibling `kithairon-link`
//! crate.

pub mod labware;
pub mod picklist;
pub mod surveys;

pub use labware::{Labware, PlateInfo};
pub use picklist::{PickList, Transfer};
pub use surveys::{PlateSurvey, WellSurvey};

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
        "labware XML could not be parsed as either ELWX or ELW.\n  ELWX error: {elwx}\n  ELW error: {elw}"
    )]
    ParseBothFormats { elwx: String, elw: String },

    #[error(
        "survey XML could not be parsed as either platesurvey or surveyreport.\n  platesurvey error: {platesurvey}\n  surveyreport error: {surveyreport}"
    )]
    ParseSurveyFormats {
        platesurvey: String,
        surveyreport: String,
    },

    #[error("a plate with type {0:?} already exists in this labware")]
    DuplicatePlateType(String),

    #[error("survey declared {declared} wells but XML contained {found}")]
    WellCountMismatch { declared: i32, found: usize },

    #[error("invalid timestamp {input:?}: {reason}")]
    InvalidTimestamp { input: String, reason: String },

    #[error("invalid well name {0:?} (expected form like \"A1\", \"D12\")")]
    InvalidWellName(String),

    #[error("invalid numeric value in survey field {0}")]
    InvalidNumber(&'static str),

    #[error("survey report contained no records")]
    EmptyReport,

    #[error("survey parquet contained no rows")]
    EmptySurveyData,

    #[error("survey parquet contains multiple surveys; differing column: {0}")]
    MultipleSurveysInParquet(&'static str),

    #[error("survey parquet is missing required column {0:?}")]
    MissingSurveyDataColumn(String),

    #[error("survey parquet contains an invalid value in {column:?}: {value}")]
    InvalidSurveyDataValue { column: String, value: String },

    #[error("survey report is inconsistent: {0} differs between records")]
    InconsistentReport(&'static str),

    #[error("picklist CSV error: {0}")]
    PickListCsv(String),

    #[error("polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),
}

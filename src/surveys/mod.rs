//! Plate survey data from the Echo instrument.
//!
//! There are two source formats:
//!   - **platesurvey.xml** — emitted directly by the Echo firmware. Contains
//!     one `<w>` element per well with volume, meniscus, and transducer data.
//!   - **surveyreport.xml** — emitted by Echo Cherry Pick software after a
//!     Cherry Pick run. Coarser: one `<record>` per well with just volume +
//!     plate identification.
//!
//! Both are parsed to the same canonical [`PlateSurvey`] representation, which
//! can then be converted to per-well records suitable for constructing a
//! Polars DataFrame in Python (or, eventually, in Rust).

pub mod platesurvey;
pub mod surveyreport;
pub mod surveydata;

pub use platesurvey::{EchoSignal, PlateSurvey, SignalFeature, WellSurvey};
pub use surveydata::{
    read_survey_parquet, read_validation_volumes_parquet, write_survey_csv, write_survey_parquet,
};

use crate::LibraryError;
use std::fs;
use std::path::Path;

/// Read a survey XML file, auto-detecting platesurvey vs surveyreport format.
/// Tries platesurvey first (the common case) and falls back to surveyreport on
/// parse failure.
pub fn read_survey_file(path: impl AsRef<Path>) -> Result<PlateSurvey, LibraryError> {
    let xml = fs::read_to_string(path.as_ref())?;
    read_survey_str(&xml)
}

pub fn read_survey_str(xml: &str) -> Result<PlateSurvey, LibraryError> {
    let trimmed = xml.trim_start_matches('\u{feff}');
    match PlateSurvey::from_platesurvey_xml(trimmed) {
        Ok(s) => Ok(s),
        Err(platesurvey_err) => match surveyreport::parse_surveyreport(trimmed) {
            Ok(s) => Ok(s),
            Err(report_err) => Err(LibraryError::ParseSurveyFormats {
                platesurvey: platesurvey_err.to_string(),
                surveyreport: report_err.to_string(),
            }),
        },
    }
}

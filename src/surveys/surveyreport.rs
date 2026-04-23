//! Parse Echo Cherry Pick `<report>` XML files into the same canonical
//! [`PlateSurvey`] representation used for `platesurvey.xml`.
//!
//! The surveyreport format is coarser: it lacks echo-signal and meniscus
//! detail, and identifies wells by `SrcWell` name (e.g. "C8") rather than
//! (row, column). Rows/columns are derived here from the well name; missing
//! metadata (vtl, original, data_format_version, meniscus_x/y, etc.) is
//! defaulted to match the Python `EchoSurveyReport.to_surveydata()` output.

use chrono::NaiveDateTime;
use quick_xml::de::from_str;
use serde::Deserialize;

use super::platesurvey::{PlateSurvey, WellSurvey};
use crate::LibraryError;

pub fn parse_surveyreport(xml: &str) -> Result<PlateSurvey, LibraryError> {
    let trimmed = xml.trim_start_matches('\u{feff}');
    let raw: RawReport = from_str(trimmed)?;
    raw.try_into()
}

#[derive(Debug, Deserialize)]
#[serde(rename = "report")]
struct RawReport {
    reportheader: RawReportHeader,
    reportbody: RawReportBody,
    #[serde(default)]
    reportfooter: Option<RawReportFooter>,
}

#[derive(Debug, Deserialize)]
struct RawReportHeader {
    #[serde(rename = "RunDateTime")]
    run_date_time: NamedValue,
}

#[derive(Debug, Deserialize)]
struct RawReportBody {
    #[serde(rename = "record", default)]
    records: Vec<RawRecord>,
}

#[derive(Debug, Deserialize)]
struct RawReportFooter {
    #[serde(rename = "InstrSN", default)]
    instr_sn: Option<NamedValue>,
}

#[derive(Debug, Deserialize)]
struct NamedValue {
    #[serde(rename = "$text", default)]
    text: String,
}

#[derive(Debug, Deserialize)]
struct RawRecord {
    #[serde(rename = "SrcPlateName")]
    src_plate_name: NamedValue,
    #[serde(rename = "SrcPlateBarcode")]
    src_plate_barcode: NamedValue,
    #[serde(rename = "SrcPlateType")]
    src_plate_type: NamedValue,
    #[serde(rename = "SrcWell")]
    src_well: NamedValue,
    #[serde(rename = "SurveyFluidHeight")]
    fluid_height: NamedValue,
    #[serde(rename = "SurveyFluidVolume")]
    fluid_volume: NamedValue,
    #[serde(rename = "FluidComposition")]
    fluid_composition: NamedValue,
    #[serde(rename = "FluidUnits")]
    fluid_units: NamedValue,
    #[serde(rename = "FluidType")]
    fluid_type: NamedValue,
    #[serde(rename = "SurveyStatus")]
    survey_status: NamedValue,
}

/// Parse "A1", "D12", etc. into (row, column), both 0-indexed.
fn parse_well_name(well: &str) -> Result<(u32, u32), LibraryError> {
    let mut chars = well.chars();
    let letter = chars
        .next()
        .ok_or_else(|| LibraryError::InvalidWellName(well.to_string()))?;
    if !letter.is_ascii_uppercase() {
        return Err(LibraryError::InvalidWellName(well.to_string()));
    }
    let row = (letter as u32) - ('A' as u32);
    let col: u32 = chars
        .as_str()
        .parse()
        .map_err(|_| LibraryError::InvalidWellName(well.to_string()))?;
    if col == 0 {
        return Err(LibraryError::InvalidWellName(well.to_string()));
    }
    Ok((row, col - 1))
}

impl TryFrom<RawReport> for PlateSurvey {
    type Error = LibraryError;

    fn try_from(raw: RawReport) -> Result<Self, Self::Error> {
        if raw.reportbody.records.is_empty() {
            return Err(LibraryError::EmptyReport);
        }

        // All records must share plate identity (the Python model enforces this).
        let first = &raw.reportbody.records[0];
        let plate_name = first.src_plate_name.text.trim().to_string();
        let plate_barcode_raw = first.src_plate_barcode.text.trim().to_string();
        let plate_type = first.src_plate_type.text.trim().to_string();

        for r in &raw.reportbody.records[1..] {
            if r.src_plate_name.text.trim() != plate_name {
                return Err(LibraryError::InconsistentReport("SrcPlateName"));
            }
            if r.src_plate_barcode.text.trim() != plate_barcode_raw {
                return Err(LibraryError::InconsistentReport("SrcPlateBarcode"));
            }
            if r.src_plate_type.text.trim() != plate_type {
                return Err(LibraryError::InconsistentReport("SrcPlateType"));
            }
        }

        let mut wells = Vec::with_capacity(raw.reportbody.records.len());
        let mut row_min = u32::MAX;
        let mut row_max = 0u32;
        let mut col_min = u32::MAX;
        let mut col_max = 0u32;

        for r in &raw.reportbody.records {
            let well_name = r.src_well.text.trim();
            let (row, column) = parse_well_name(well_name)?;
            row_min = row_min.min(row);
            row_max = row_max.max(row);
            col_min = col_min.min(column);
            col_max = col_max.max(column);
            let volume: f64 = r
                .fluid_volume
                .text
                .trim()
                .parse()
                .map_err(|_| LibraryError::InvalidNumber("SurveyFluidVolume"))?;
            let fluid_height: f64 = r
                .fluid_height
                .text
                .trim()
                .parse()
                .map_err(|_| LibraryError::InvalidNumber("SurveyFluidHeight"))?;
            let fluid_composition: f64 = r
                .fluid_composition
                .text
                .trim()
                .parse()
                .map_err(|_| LibraryError::InvalidNumber("FluidComposition"))?;

            wells.push(WellSurvey {
                row,
                column,
                well: well_name.to_string(),
                volume: if volume == 0.0 { None } else { Some(volume) },
                current_volume: None,
                status: r.survey_status.text.trim().to_string(),
                fluid: r.fluid_type.text.trim().to_string(),
                fluid_units: r.fluid_units.text.trim().to_string(),
                meniscus_x: 0.0,
                meniscus_y: 0.0,
                fluid_composition,
                dmso_homogeneous: 0.0,
                dmso_inhomogeneous: 0.0,
                fluid_thickness: fluid_height,
                current_fluid_thickness: 0.0,
                bottom_thickness: 0.0,
                fluid_thickness_homogeneous: 0.0,
                fluid_thickness_imhomogeneous: 0.0,
                outlier: 0.0,
                corrective_action: String::new(),
                echo_signal: None,
            });
        }

        let timestamp = NaiveDateTime::parse_from_str(
            raw.reportheader.run_date_time.text.trim(),
            "%Y-%m-%d %H:%M:%S",
        )
        .map_err(|e| LibraryError::InvalidTimestamp {
            input: raw.reportheader.run_date_time.text.clone(),
            reason: e.to_string(),
        })?;

        let instrument_serial_number = raw
            .reportfooter
            .as_ref()
            .and_then(|f| f.instr_sn.as_ref())
            .map(|v| v.text.trim().to_string())
            .unwrap_or_default();

        let survey_rows = (row_max - row_min + 1) as i32;
        let survey_columns = (col_max - col_min + 1) as i32;

        Ok(PlateSurvey {
            plate_type,
            plate_barcode: if plate_barcode_raw == "UnknownBarCode" || plate_barcode_raw.is_empty()
            {
                None
            } else {
                Some(plate_barcode_raw)
            },
            timestamp,
            instrument_serial_number,
            vtl: 0,
            original: 0,
            data_format_version: 1,
            survey_rows,
            survey_columns,
            survey_total_wells: wells.len() as i32,
            plate_name: Some(plate_name),
            comment: None,
            wells,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SURVEY_REPORT_XML: &str =
        include_str!("../../tests/test_data/surveyreport-cp.xml");

    #[test]
    fn parses_surveyreport_fixture() {
        let ps = parse_surveyreport(SURVEY_REPORT_XML).expect("parse");
        assert!(!ps.wells.is_empty());
        assert_eq!(ps.plate_type, "384PP_AQ_BP");
        assert_eq!(ps.plate_name.as_deref(), Some("GD_fls"));
    }

    #[test]
    fn surveyreport_well_names_decode_correctly() {
        let ps = parse_surveyreport(SURVEY_REPORT_XML).expect("parse");
        let c8 = ps.wells.iter().find(|w| w.well == "C8").expect("C8 present");
        // C8 → row 2 (A=0,B=1,C=2), column 7 (0-indexed from 8)
        assert_eq!(c8.row, 2);
        assert_eq!(c8.column, 7);
        assert!(c8.volume.is_some());
    }

    #[test]
    fn well_name_parse_round_trip() {
        assert_eq!(parse_well_name("A1").unwrap(), (0, 0));
        assert_eq!(parse_well_name("P24").unwrap(), (15, 23));
        assert_eq!(parse_well_name("D12").unwrap(), (3, 11));
        assert!(parse_well_name("").is_err());
        assert!(parse_well_name("A0").is_err());
        assert!(parse_well_name("a1").is_err());
    }
}

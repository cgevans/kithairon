//! Parse Echo `<platesurvey>` XML files into the canonical [`PlateSurvey`]
//! representation.
//!
//! The Echo firmware emits one `<platesurvey>` per survey, with one `<w>`
//! child per well. Each `<w>` has an optional `<e>` (echo signal) child which
//! in turn contains `<f>` (feature) grandchildren.

use chrono::NaiveDateTime;
use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};

use crate::LibraryError;

/// Canonical in-memory representation of an Echo plate survey.
///
/// All fields mirror the Python `EchoPlateSurveyXML` model so the per-well
/// records produced by [`Self::to_records`] match the column names downstream
/// Python code (and tests) expect.
#[derive(Debug, Clone, PartialEq)]
pub struct PlateSurvey {
    pub plate_type: String,
    /// Normalized: the literal string "UnknownBarCode" parses to `None`.
    pub plate_barcode: Option<String>,
    /// Timestamp string in the format the XML uses ("YYYY-MM-DD HH:MM:SS.sss").
    /// Kept as-is here; the PyO3 layer converts to a Python `datetime`.
    pub timestamp: NaiveDateTime,
    pub instrument_serial_number: String,
    pub vtl: i32,
    pub original: i32,
    pub data_format_version: i32,
    pub survey_rows: i32,
    pub survey_columns: i32,
    pub survey_total_wells: i32,
    pub plate_name: Option<String>,
    pub comment: Option<String>,
    pub wells: Vec<WellSurvey>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WellSurvey {
    pub row: u32,
    pub column: u32,
    pub well: String,
    /// 0.0 in XML is treated as "measurement unavailable" and maps to `None`.
    pub volume: Option<f64>,
    pub current_volume: Option<f64>,
    pub status: String,
    pub fluid: String,
    pub fluid_units: String,
    pub meniscus_x: f64,
    pub meniscus_y: f64,
    pub fluid_composition: f64,
    pub dmso_homogeneous: f64,
    pub dmso_inhomogeneous: f64,
    pub fluid_thickness: f64,
    pub current_fluid_thickness: f64,
    pub bottom_thickness: f64,
    pub fluid_thickness_homogeneous: f64,
    pub fluid_thickness_imhomogeneous: f64,
    pub outlier: f64,
    pub corrective_action: String,
    pub echo_signal: Option<EchoSignal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EchoSignal {
    pub signal_type: String,
    pub transducer_x: f64,
    pub transducer_y: f64,
    pub transducer_z: f64,
    pub features: Vec<SignalFeature>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignalFeature {
    pub feature_type: String,
    pub tof: f64,
    pub vpp: f64,
}

impl PlateSurvey {
    pub fn from_platesurvey_xml(xml: &str) -> Result<Self, LibraryError> {
        let trimmed = xml.trim_start_matches('\u{feff}');
        let raw: RawPlateSurvey = from_str(trimmed)?;
        raw.try_into()
    }

    /// Serialize back to the firmware's `<platesurvey>` XML form.
    ///
    /// Inverse of [`Self::from_platesurvey_xml`]. The fixture-based test
    /// `platesurvey_round_trip_*` covers the parse → serialize → parse
    /// equivalence on a real plate survey emitted by the instrument.
    /// Per-well timestamps in the original may not reach byte-identical
    /// because the parser normalises a few quirks (`vl="0"` ↔ `None`),
    /// but the parsed-data round-trip is exact.
    pub fn to_platesurvey_xml(&self) -> Result<String, LibraryError> {
        let raw = RawPlateSurvey::from(self);
        let mut buf = String::new();
        let ser = quick_xml::se::Serializer::with_root(&mut buf, Some("platesurvey"))?;
        raw.serialize(ser)?;
        Ok(buf)
    }
}

// ---------------------------------------------------------------------------
// XML-binding types.

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename = "platesurvey")]
struct RawPlateSurvey {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@barcode")]
    barcode: String,
    #[serde(rename = "@date")]
    date: String,
    #[serde(rename = "@serial_number")]
    serial_number: String,
    #[serde(rename = "@vtl")]
    vtl: i32,
    #[serde(rename = "@original")]
    original: i32,
    #[serde(rename = "@frmt")]
    frmt: i32,
    #[serde(rename = "@rows")]
    rows: i32,
    #[serde(rename = "@cols")]
    cols: i32,
    #[serde(rename = "@totalWells")]
    total_wells: i32,
    #[serde(rename = "@plate_name", default, skip_serializing_if = "Option::is_none")]
    plate_name: Option<String>,
    #[serde(rename = "@note", default, skip_serializing_if = "Option::is_none")]
    note: Option<String>,
    #[serde(rename = "w", default)]
    wells: Vec<RawWell>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RawWell {
    #[serde(rename = "@r")]
    r: u32,
    #[serde(rename = "@c")]
    c: u32,
    #[serde(rename = "@n")]
    n: String,
    #[serde(rename = "@vl")]
    vl: f64,
    #[serde(rename = "@cvl")]
    cvl: f64,
    #[serde(rename = "@status", default)]
    status: String,
    #[serde(rename = "@fld")]
    fld: String,
    #[serde(rename = "@fldu", default)]
    fldu: String,
    #[serde(rename = "@x")]
    x: f64,
    #[serde(rename = "@y")]
    y: f64,
    #[serde(rename = "@s")]
    s: f64,
    #[serde(rename = "@fsh")]
    fsh: f64,
    #[serde(rename = "@fsinh")]
    fsinh: f64,
    #[serde(rename = "@t")]
    t: f64,
    #[serde(rename = "@ct")]
    ct: f64,
    #[serde(rename = "@b")]
    b: f64,
    #[serde(rename = "@fth")]
    fth: f64,
    #[serde(rename = "@ftinh")]
    ftinh: f64,
    #[serde(rename = "@o")]
    o: f64,
    #[serde(rename = "@a")]
    a: String,
    #[serde(rename = "e", default, skip_serializing_if = "Option::is_none")]
    echo_signal: Option<RawEchoSignal>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RawEchoSignal {
    #[serde(rename = "@t")]
    t: String,
    #[serde(rename = "@x")]
    x: f64,
    #[serde(rename = "@y")]
    y: f64,
    #[serde(rename = "@z")]
    z: f64,
    #[serde(rename = "f", default)]
    features: Vec<RawFeature>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RawFeature {
    #[serde(rename = "@t")]
    t: String,
    #[serde(rename = "@o")]
    o: f64,
    #[serde(rename = "@v")]
    v: f64,
}

// ---------------------------------------------------------------------------
// Normalization: RawPlateSurvey → PlateSurvey.

fn null_zero(v: f64) -> Option<f64> {
    if v == 0.0 { None } else { Some(v) }
}

fn parse_timestamp(s: &str) -> Result<NaiveDateTime, LibraryError> {
    // Echo emits "YYYY-MM-DD HH:MM:SS.sss"; try with and without fractional
    // seconds so older files without ".sss" also parse.
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| LibraryError::InvalidTimestamp {
            input: s.to_string(),
            reason: e.to_string(),
        })
}

impl TryFrom<RawPlateSurvey> for PlateSurvey {
    type Error = LibraryError;

    fn try_from(raw: RawPlateSurvey) -> Result<Self, Self::Error> {
        if raw.wells.len() != raw.total_wells as usize {
            return Err(LibraryError::WellCountMismatch {
                declared: raw.total_wells,
                found: raw.wells.len(),
            });
        }
        let timestamp = parse_timestamp(&raw.date)?;
        let wells = raw.wells.into_iter().map(WellSurvey::from).collect();
        Ok(Self {
            plate_type: raw.name,
            plate_barcode: if raw.barcode == "UnknownBarCode" {
                None
            } else {
                Some(raw.barcode)
            },
            timestamp,
            instrument_serial_number: raw.serial_number,
            vtl: raw.vtl,
            original: raw.original,
            data_format_version: raw.frmt,
            survey_rows: raw.rows,
            survey_columns: raw.cols,
            survey_total_wells: raw.total_wells,
            plate_name: raw.plate_name,
            comment: raw.note,
            wells,
        })
    }
}

impl From<RawWell> for WellSurvey {
    fn from(r: RawWell) -> Self {
        Self {
            row: r.r,
            column: r.c,
            well: r.n,
            volume: null_zero(r.vl),
            current_volume: null_zero(r.cvl),
            status: r.status,
            fluid: r.fld,
            fluid_units: r.fldu,
            meniscus_x: r.x,
            meniscus_y: r.y,
            fluid_composition: r.s,
            dmso_homogeneous: r.fsh,
            dmso_inhomogeneous: r.fsinh,
            fluid_thickness: r.t,
            current_fluid_thickness: r.ct,
            bottom_thickness: r.b,
            fluid_thickness_homogeneous: r.fth,
            fluid_thickness_imhomogeneous: r.ftinh,
            outlier: r.o,
            corrective_action: r.a,
            echo_signal: r.echo_signal.map(EchoSignal::from),
        }
    }
}

impl From<RawEchoSignal> for EchoSignal {
    fn from(r: RawEchoSignal) -> Self {
        Self {
            signal_type: r.t,
            transducer_x: r.x,
            transducer_y: r.y,
            transducer_z: r.z,
            features: r.features.into_iter().map(SignalFeature::from).collect(),
        }
    }
}

impl From<RawFeature> for SignalFeature {
    fn from(r: RawFeature) -> Self {
        Self {
            feature_type: r.t,
            tof: r.o,
            vpp: r.v,
        }
    }
}

// ---------------------------------------------------------------------------
// PlateSurvey → RawPlateSurvey for serialize.
//
// Mirrors `RawPlateSurvey → PlateSurvey` field-for-field. Two
// normalisations are reversed at this boundary so the wire form matches
// what the firmware emits:
//
//   - `Option<volume>` (None means "missing measurement") → `0.0`.
//   - `Option<plate_barcode>` (None means absent) → `"UnknownBarCode"`.
//
// Timestamps reformat to "YYYY-MM-DD HH:MM:SS%.3f" — the same shape
// `parse_timestamp` accepts.

fn unnull_zero(v: Option<f64>) -> f64 {
    v.unwrap_or(0.0)
}

fn format_timestamp(ts: &NaiveDateTime) -> String {
    ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

impl From<&PlateSurvey> for RawPlateSurvey {
    fn from(p: &PlateSurvey) -> Self {
        Self {
            name: p.plate_type.clone(),
            barcode: p
                .plate_barcode
                .clone()
                .unwrap_or_else(|| "UnknownBarCode".to_string()),
            date: format_timestamp(&p.timestamp),
            serial_number: p.instrument_serial_number.clone(),
            vtl: p.vtl,
            original: p.original,
            frmt: p.data_format_version,
            rows: p.survey_rows,
            cols: p.survey_columns,
            total_wells: p.survey_total_wells,
            plate_name: p.plate_name.clone(),
            note: p.comment.clone(),
            wells: p.wells.iter().map(RawWell::from).collect(),
        }
    }
}

impl From<&WellSurvey> for RawWell {
    fn from(w: &WellSurvey) -> Self {
        Self {
            r: w.row,
            c: w.column,
            n: w.well.clone(),
            vl: unnull_zero(w.volume),
            cvl: unnull_zero(w.current_volume),
            status: w.status.clone(),
            fld: w.fluid.clone(),
            fldu: w.fluid_units.clone(),
            x: w.meniscus_x,
            y: w.meniscus_y,
            s: w.fluid_composition,
            fsh: w.dmso_homogeneous,
            fsinh: w.dmso_inhomogeneous,
            t: w.fluid_thickness,
            ct: w.current_fluid_thickness,
            b: w.bottom_thickness,
            fth: w.fluid_thickness_homogeneous,
            ftinh: w.fluid_thickness_imhomogeneous,
            o: w.outlier,
            a: w.corrective_action.clone(),
            echo_signal: w.echo_signal.as_ref().map(RawEchoSignal::from),
        }
    }
}

impl From<&EchoSignal> for RawEchoSignal {
    fn from(s: &EchoSignal) -> Self {
        Self {
            t: s.signal_type.clone(),
            x: s.transducer_x,
            y: s.transducer_y,
            z: s.transducer_z,
            features: s.features.iter().map(RawFeature::from).collect(),
        }
    }
}

impl From<&SignalFeature> for RawFeature {
    fn from(f: &SignalFeature) -> Self {
        Self {
            t: f.feature_type.clone(),
            o: f.tof,
            v: f.vpp,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {
    use super::*;

    const PLATESURVEY_XML: &str = include_str!("../../tests/test_data/platesurvey.xml");

    #[test]
    fn parses_platesurvey_fixture() {
        let ps = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        assert_eq!(ps.plate_type, "384PP_AQ_BP");
        assert!(ps.plate_barcode.is_none()); // "UnknownBarCode"
        assert_eq!(ps.survey_rows, 16);
        assert_eq!(ps.survey_columns, 24);
        assert_eq!(ps.survey_total_wells, 384);
        assert_eq!(ps.wells.len(), 384);
        assert_eq!(ps.instrument_serial_number, "E5XX-12345");
        assert_eq!(ps.data_format_version, 1);
    }

    #[test]
    fn platesurvey_well_zero_volume_becomes_none() {
        let ps = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        // A2 has vl="0" and a "Data missing" status per the fixture.
        let a2 = ps.wells.iter().find(|w| w.well == "A2").expect("A2 present");
        assert!(a2.volume.is_none());
        assert!(a2.status.contains("Data missing"));
    }

    #[test]
    fn platesurvey_well_echo_signal_features_preserved() {
        let ps = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        let a1 = ps.wells.iter().find(|w| w.well == "A1").expect("A1 present");
        let signal = a1.echo_signal.as_ref().expect("A1 has an echo signal");
        assert_eq!(signal.signal_type, "AVG");
        assert_eq!(signal.features.len(), 3);
        assert_eq!(signal.features[0].feature_type, "FW BB");
    }

    #[test]
    fn platesurvey_round_trip_preserves_parsed_data() {
        // Parse → serialize → re-parse: the second parsed value must
        // equal the first. We don't compare XML strings byte-for-byte
        // because the parser normalises a few quirks (vl="0" ↔ None).
        let original = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        let serialized = original
            .to_platesurvey_xml()
            .expect("serialize");
        let reparsed = PlateSurvey::from_platesurvey_xml(&serialized).expect("re-parse");
        assert_eq!(reparsed, original);
    }

    #[test]
    fn platesurvey_round_trip_preserves_optional_metadata() {
        // Drive the Some-for-plate_name/comment/echo_signal branch.
        let mut ps = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        ps.plate_name = Some("source1".into());
        ps.comment = Some("before".into());
        ps.plate_barcode = Some("BC-12345".into());
        let serialized = ps.to_platesurvey_xml().expect("serialize");
        let reparsed = PlateSurvey::from_platesurvey_xml(&serialized).expect("re-parse");
        assert_eq!(reparsed.plate_name.as_deref(), Some("source1"));
        assert_eq!(reparsed.comment.as_deref(), Some("before"));
        assert_eq!(reparsed.plate_barcode.as_deref(), Some("BC-12345"));
    }

    #[test]
    fn platesurvey_round_trip_unknown_barcode_normalises_to_none() {
        // Round-tripping a survey with no barcode keeps it None on the
        // Rust side (the wire form puts back "UnknownBarCode").
        let original = PlateSurvey::from_platesurvey_xml(PLATESURVEY_XML).expect("parse");
        assert!(original.plate_barcode.is_none());
        let serialized = original.to_platesurvey_xml().expect("serialize");
        assert!(serialized.contains(r#"barcode="UnknownBarCode""#));
        let reparsed = PlateSurvey::from_platesurvey_xml(&serialized).expect("re-parse");
        assert!(reparsed.plate_barcode.is_none());
    }

    #[test]
    fn well_count_mismatch_rejected() {
        let truncated = PLATESURVEY_XML.replace(r#"totalWells="384""#, r#"totalWells="999""#);
        let result = PlateSurvey::from_platesurvey_xml(&truncated);
        assert!(matches!(
            result,
            Err(LibraryError::WellCountMismatch { declared: 999, .. })
        ));
    }
}

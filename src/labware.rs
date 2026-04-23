//! Labware definitions — reading and writing Echo ELW / ELWX XML files.
//!
//! The Echo manufacturer software defines plate types in two formats:
//!   - **ELW** (older): source and destination plates live under separate tags;
//!     `usage` is implicit (from the container), `plateformat` is absent, and
//!     `welllength` is assumed equal to `wellwidth`.
//!   - **ELWX** (current): every `plateinfo` carries `plateformat`, explicit
//!     `usage`, and `welllength`. This is the canonical format we write.
//!
//! [`Labware`] holds a flat list of [`PlateInfo`]. Reading either format
//! produces a canonical `PlateInfo` with all fields populated.

use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::LibraryError;

/// A single Echo plate-type definition.
///
/// Field names match the Python `PlateInfo` on `kithairon.labware` for
/// one-to-one parity. The XML attribute names (lowercase, no underscores)
/// are applied via serde rename on the intermediate `RawPlateInfo` types,
/// not on this struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlateInfo {
    pub plate_type: String,
    pub plate_format: String,
    pub usage: String,
    pub fluid: Option<String>,
    pub manufacturer: String,
    pub lot_number: String,
    pub part_number: String,
    pub rows: u32,
    pub cols: u32,
    pub a1_offset_y: i32,
    pub center_spacing_x: i32,
    pub center_spacing_y: i32,
    pub plate_height: i32,
    pub skirt_height: i32,
    pub well_width: i32,
    pub well_length: i32,
    pub well_capacity: i32,
    pub bottom_inset: f64,
    pub center_well_pos_x: f64,
    pub center_well_pos_y: f64,
    pub min_well_vol: Option<f64>,
    pub max_well_vol: Option<f64>,
    pub max_vol_total: Option<f64>,
    pub min_volume: Option<f64>,
    pub drop_volume: Option<f64>,
}

impl PlateInfo {
    pub fn shape(&self) -> (u32, u32) {
        (self.rows, self.cols)
    }
}

/// A collection of plate-type definitions, typically loaded from an ELWX file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Labware {
    plates: Vec<PlateInfo>,
}

impl Labware {
    pub fn new(plates: Vec<PlateInfo>) -> Self {
        Self { plates }
    }

    pub fn plates(&self) -> &[PlateInfo] {
        &self.plates
    }

    pub fn into_plates(self) -> Vec<PlateInfo> {
        self.plates
    }

    pub fn keys(&self) -> Vec<&str> {
        self.plates.iter().map(|p| p.plate_type.as_str()).collect()
    }

    pub fn get(&self, plate_type: &str) -> Option<&PlateInfo> {
        self.plates.iter().find(|p| p.plate_type == plate_type)
    }

    pub fn add(&mut self, plate: PlateInfo) -> Result<(), LibraryError> {
        if self.get(&plate.plate_type).is_some() {
            return Err(LibraryError::DuplicatePlateType(plate.plate_type));
        }
        self.plates.push(plate);
        Ok(())
    }

    /// Parse an ELWX (or ELW) XML string. ELWX is tried first; on failure,
    /// the string is reparsed as ELW.
    pub fn from_xml_str(xml: &str) -> Result<Self, LibraryError> {
        // BOMs from Windows-produced files confuse quick-xml.
        let trimmed = xml.trim_start_matches('\u{feff}');
        match from_str::<RawLabwareElwx>(trimmed) {
            Ok(raw) => Ok(Self::from_raw_elwx(raw)),
            Err(elwx_err) => match from_str::<RawLabwareElw>(trimmed) {
                Ok(raw) => Ok(Self::from_raw_elw(raw)),
                Err(elw_err) => Err(LibraryError::ParseBothFormats {
                    elwx: elwx_err.to_string(),
                    elw: elw_err.to_string(),
                }),
            },
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, LibraryError> {
        let xml = fs::read_to_string(path.as_ref())?;
        Self::from_xml_str(&xml)
    }

    /// Serialize to ELWX XML. Missing optional numeric fields are omitted.
    pub fn to_elwx_string(&self) -> Result<String, LibraryError> {
        let raw = RawLabwareElwx {
            source_plates: RawPlateListElwx {
                plates: self
                    .plates
                    .iter()
                    .filter(|p| p.usage == "SRC")
                    .map(RawPlateInfoElwx::from)
                    .collect(),
            },
            destination_plates: RawPlateListElwx {
                plates: self
                    .plates
                    .iter()
                    .filter(|p| p.usage == "DEST")
                    .map(RawPlateInfoElwx::from)
                    .collect(),
            },
        };
        let mut buf = String::from(
            "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>\n",
        );
        let mut ser = quick_xml::se::Serializer::with_root(&mut buf, Some("EchoLabware"))?;
        ser.indent(' ', 2);
        raw.serialize(ser)?;
        Ok(buf)
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), LibraryError> {
        let xml = self.to_elwx_string()?;
        fs::write(path.as_ref(), xml)?;
        Ok(())
    }

    fn from_raw_elwx(raw: RawLabwareElwx) -> Self {
        let mut plates = Vec::with_capacity(
            raw.source_plates.plates.len() + raw.destination_plates.plates.len(),
        );
        plates.extend(raw.source_plates.plates.into_iter().map(PlateInfo::from));
        plates.extend(raw.destination_plates.plates.into_iter().map(PlateInfo::from));
        Self { plates }
    }

    fn from_raw_elw(raw: RawLabwareElw) -> Self {
        let mut plates = Vec::with_capacity(
            raw.source_plates.plates.len() + raw.destination_plates.plates.len(),
        );
        for p in raw.source_plates.plates {
            plates.push(PlateInfo::from_elw_raw(p, "SRC"));
        }
        for p in raw.destination_plates.plates {
            plates.push(PlateInfo::from_elw_raw(p, "DEST"));
        }
        Self { plates }
    }
}

impl std::ops::Index<&str> for Labware {
    type Output = PlateInfo;
    fn index(&self, plate_type: &str) -> &PlateInfo {
        self.get(plate_type)
            .unwrap_or_else(|| panic!("no plate type: {plate_type}"))
    }
}

// ---------------------------------------------------------------------------
// Intermediate XML-binding types. Kept private; the public surface is the
// canonical `PlateInfo` / `Labware`.

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "EchoLabware")]
struct RawLabwareElwx {
    #[serde(rename = "sourceplates")]
    source_plates: RawPlateListElwx,
    #[serde(rename = "destinationplates")]
    destination_plates: RawPlateListElwx,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawPlateListElwx {
    #[serde(rename = "plateinfo", default)]
    plates: Vec<RawPlateInfoElwx>,
}

/// ELWX-format plateinfo: every structural attribute is present.
#[derive(Debug, Serialize, Deserialize)]
struct RawPlateInfoElwx {
    #[serde(rename = "@platetype")]
    plate_type: String,
    #[serde(rename = "@plateformat")]
    plate_format: String,
    #[serde(rename = "@usage")]
    usage: String,
    #[serde(rename = "@fluid", skip_serializing_if = "Option::is_none")]
    fluid: Option<String>,
    #[serde(rename = "@manufacturer")]
    manufacturer: String,
    #[serde(rename = "@lotnumber")]
    lot_number: String,
    #[serde(rename = "@partnumber")]
    part_number: String,
    #[serde(rename = "@rows")]
    rows: u32,
    #[serde(rename = "@cols")]
    cols: u32,
    #[serde(rename = "@a1offsety")]
    a1_offset_y: i32,
    #[serde(rename = "@centerspacingx")]
    center_spacing_x: i32,
    #[serde(rename = "@centerspacingy")]
    center_spacing_y: i32,
    #[serde(rename = "@plateheight")]
    plate_height: i32,
    #[serde(rename = "@skirtheight")]
    skirt_height: i32,
    #[serde(rename = "@wellwidth")]
    well_width: i32,
    #[serde(rename = "@welllength")]
    well_length: i32,
    #[serde(rename = "@wellcapacity")]
    well_capacity: i32,
    #[serde(rename = "@bottominset")]
    bottom_inset: f64,
    #[serde(rename = "@centerwellposx")]
    center_well_pos_x: f64,
    #[serde(rename = "@centerwellposy")]
    center_well_pos_y: f64,
    #[serde(rename = "@minwellvol", skip_serializing_if = "Option::is_none")]
    min_well_vol: Option<f64>,
    #[serde(rename = "@maxwellvol", skip_serializing_if = "Option::is_none")]
    max_well_vol: Option<f64>,
    #[serde(rename = "@maxvoltotal", skip_serializing_if = "Option::is_none")]
    max_vol_total: Option<f64>,
    #[serde(rename = "@minvolume", skip_serializing_if = "Option::is_none")]
    min_volume: Option<f64>,
    #[serde(rename = "@dropvolume", skip_serializing_if = "Option::is_none")]
    drop_volume: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "EchoLabware")]
struct RawLabwareElw {
    #[serde(rename = "sourceplates")]
    source_plates: RawPlateListElw,
    #[serde(rename = "destinationplates")]
    destination_plates: RawPlateListElw,
}

#[derive(Debug, Deserialize)]
struct RawPlateListElw {
    #[serde(rename = "plateinfo", default)]
    plates: Vec<RawPlateInfoElw>,
}

/// ELW-format plateinfo: lacks plateformat, usage, welllength, fluid.
#[derive(Debug, Deserialize)]
struct RawPlateInfoElw {
    #[serde(rename = "@platetype")]
    plate_type: String,
    #[serde(rename = "@manufacturer")]
    manufacturer: String,
    #[serde(rename = "@lotnumber")]
    lot_number: String,
    #[serde(rename = "@partnumber")]
    part_number: String,
    #[serde(rename = "@rows")]
    rows: u32,
    #[serde(rename = "@cols")]
    cols: u32,
    #[serde(rename = "@a1offsety")]
    a1_offset_y: i32,
    #[serde(rename = "@centerspacingx")]
    center_spacing_x: i32,
    #[serde(rename = "@centerspacingy")]
    center_spacing_y: i32,
    #[serde(rename = "@plateheight")]
    plate_height: i32,
    #[serde(rename = "@skirtheight")]
    skirt_height: i32,
    #[serde(rename = "@wellwidth")]
    well_width: i32,
    #[serde(rename = "@wellcapacity")]
    well_capacity: i32,
    #[serde(rename = "@bottominset")]
    bottom_inset: f64,
    #[serde(rename = "@centerwellposx")]
    center_well_pos_x: f64,
    #[serde(rename = "@centerwellposy")]
    center_well_pos_y: f64,
    #[serde(rename = "@minwellvol", default)]
    min_well_vol: Option<f64>,
    #[serde(rename = "@maxwellvol", default)]
    max_well_vol: Option<f64>,
    #[serde(rename = "@maxvoltotal", default)]
    max_vol_total: Option<f64>,
    #[serde(rename = "@minvolume", default)]
    min_volume: Option<f64>,
    #[serde(rename = "@dropvolume", default)]
    drop_volume: Option<f64>,
}

impl From<RawPlateInfoElwx> for PlateInfo {
    fn from(r: RawPlateInfoElwx) -> Self {
        Self {
            plate_type: r.plate_type,
            plate_format: r.plate_format,
            usage: r.usage,
            fluid: r.fluid,
            manufacturer: r.manufacturer,
            lot_number: r.lot_number,
            part_number: r.part_number,
            rows: r.rows,
            cols: r.cols,
            a1_offset_y: r.a1_offset_y,
            center_spacing_x: r.center_spacing_x,
            center_spacing_y: r.center_spacing_y,
            plate_height: r.plate_height,
            skirt_height: r.skirt_height,
            well_width: r.well_width,
            well_length: r.well_length,
            well_capacity: r.well_capacity,
            bottom_inset: r.bottom_inset,
            center_well_pos_x: r.center_well_pos_x,
            center_well_pos_y: r.center_well_pos_y,
            min_well_vol: r.min_well_vol,
            max_well_vol: r.max_well_vol,
            max_vol_total: r.max_vol_total,
            min_volume: r.min_volume,
            drop_volume: r.drop_volume,
        }
    }
}

impl From<&PlateInfo> for RawPlateInfoElwx {
    fn from(p: &PlateInfo) -> Self {
        Self {
            plate_type: p.plate_type.clone(),
            plate_format: p.plate_format.clone(),
            usage: p.usage.clone(),
            fluid: p.fluid.clone(),
            manufacturer: p.manufacturer.clone(),
            lot_number: p.lot_number.clone(),
            part_number: p.part_number.clone(),
            rows: p.rows,
            cols: p.cols,
            a1_offset_y: p.a1_offset_y,
            center_spacing_x: p.center_spacing_x,
            center_spacing_y: p.center_spacing_y,
            plate_height: p.plate_height,
            skirt_height: p.skirt_height,
            well_width: p.well_width,
            well_length: p.well_length,
            well_capacity: p.well_capacity,
            bottom_inset: p.bottom_inset,
            center_well_pos_x: p.center_well_pos_x,
            center_well_pos_y: p.center_well_pos_y,
            min_well_vol: p.min_well_vol,
            max_well_vol: p.max_well_vol,
            max_vol_total: p.max_vol_total,
            min_volume: p.min_volume,
            drop_volume: p.drop_volume,
        }
    }
}

impl PlateInfo {
    fn from_elw_raw(r: RawPlateInfoElw, usage: &str) -> Self {
        Self {
            plate_type: r.plate_type,
            plate_format: "UNKNOWN".to_string(),
            usage: usage.to_string(),
            fluid: None,
            manufacturer: r.manufacturer,
            lot_number: r.lot_number,
            part_number: r.part_number,
            rows: r.rows,
            cols: r.cols,
            a1_offset_y: r.a1_offset_y,
            center_spacing_x: r.center_spacing_x,
            center_spacing_y: r.center_spacing_y,
            plate_height: r.plate_height,
            skirt_height: r.skirt_height,
            well_width: r.well_width,
            well_length: r.well_width,
            well_capacity: r.well_capacity,
            bottom_inset: r.bottom_inset,
            center_well_pos_x: r.center_well_pos_x,
            center_well_pos_y: r.center_well_pos_y,
            min_well_vol: r.min_well_vol,
            max_well_vol: r.max_well_vol,
            max_vol_total: r.max_vol_total,
            min_volume: r.min_volume,
            drop_volume: r.drop_volume,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {
    use super::*;

    const ELWX_FIXTURE: &str = include_str!("../tests/test_data/Labware.elwx");
    const ELW_FIXTURE: &str = include_str!("../tests/test_data/Labware.elw");

    #[test]
    fn parses_elwx_fixture() {
        let lw = Labware::from_xml_str(ELWX_FIXTURE).expect("parse ELWX");
        assert!(!lw.plates().is_empty());
        let p = lw.get("384LDV_Plus_AQ_GP").expect("plate present");
        assert_eq!(p.plate_format, "384LDV");
        assert_eq!(p.usage, "SRC");
        assert_eq!(p.rows, 16);
        assert_eq!(p.cols, 24);
        assert_eq!(p.drop_volume, Some(25.0));
        assert_eq!(p.fluid.as_deref(), Some("Glycerol"));
    }

    #[test]
    fn parses_elw_fixture() {
        let lw = Labware::from_xml_str(ELW_FIXTURE).expect("parse ELW");
        assert!(!lw.plates().is_empty());
        let p = lw.get("Corning_1536COC_HiBase").expect("plate present");
        assert_eq!(p.plate_format, "UNKNOWN");
        assert_eq!(p.usage, "SRC");
        assert_eq!(p.well_length, p.well_width);
        assert_eq!(p.drop_volume, Some(2.5));
    }

    #[test]
    fn elwx_round_trip_keeps_all_plates() {
        let lw = Labware::from_xml_str(ELWX_FIXTURE).expect("parse");
        let serialized = lw.to_elwx_string().expect("serialize");
        let lw2 = Labware::from_xml_str(&serialized).expect("reparse");
        assert_eq!(lw, lw2);
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let lw = Labware::from_xml_str(ELWX_FIXTURE).expect("parse");
        assert!(lw.get("does-not-exist").is_none());
    }

    #[test]
    fn add_refuses_duplicate() {
        let mut lw = Labware::from_xml_str(ELWX_FIXTURE).expect("parse");
        let existing = lw.plates()[0].clone();
        assert!(matches!(
            lw.add(existing),
            Err(LibraryError::DuplicatePlateType(_))
        ));
    }
}

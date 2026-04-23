//! PyO3 bindings exposing the Rust API to Python as `kithairon._native`.

use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::labware::{Labware, PlateInfo};
use crate::surveys::platesurvey::{EchoSignal, PlateSurvey, SignalFeature, WellSurvey};
use crate::surveys::{read_survey_file, read_survey_str};

#[pyclass(name = "PlateInfo", module = "kithairon._native", frozen, from_py_object)]
#[derive(Clone)]
struct PyPlateInfo {
    inner: PlateInfo,
}

#[pymethods]
impl PyPlateInfo {
    #[getter]
    fn plate_type(&self) -> &str {
        &self.inner.plate_type
    }
    #[getter]
    fn plate_format(&self) -> &str {
        &self.inner.plate_format
    }
    #[getter]
    fn usage(&self) -> &str {
        &self.inner.usage
    }
    #[getter]
    fn fluid(&self) -> Option<&str> {
        self.inner.fluid.as_deref()
    }
    #[getter]
    fn manufacturer(&self) -> &str {
        &self.inner.manufacturer
    }
    #[getter]
    fn lot_number(&self) -> &str {
        &self.inner.lot_number
    }
    #[getter]
    fn part_number(&self) -> &str {
        &self.inner.part_number
    }
    #[getter]
    fn rows(&self) -> u32 {
        self.inner.rows
    }
    #[getter]
    fn cols(&self) -> u32 {
        self.inner.cols
    }
    #[getter]
    fn a1_offset_y(&self) -> i32 {
        self.inner.a1_offset_y
    }
    #[getter]
    fn center_spacing_x(&self) -> i32 {
        self.inner.center_spacing_x
    }
    #[getter]
    fn center_spacing_y(&self) -> i32 {
        self.inner.center_spacing_y
    }
    #[getter]
    fn plate_height(&self) -> i32 {
        self.inner.plate_height
    }
    #[getter]
    fn skirt_height(&self) -> i32 {
        self.inner.skirt_height
    }
    #[getter]
    fn well_width(&self) -> i32 {
        self.inner.well_width
    }
    #[getter]
    fn well_length(&self) -> i32 {
        self.inner.well_length
    }
    #[getter]
    fn well_capacity(&self) -> i32 {
        self.inner.well_capacity
    }
    #[getter]
    fn bottom_inset(&self) -> f64 {
        self.inner.bottom_inset
    }
    #[getter]
    fn center_well_pos_x(&self) -> f64 {
        self.inner.center_well_pos_x
    }
    #[getter]
    fn center_well_pos_y(&self) -> f64 {
        self.inner.center_well_pos_y
    }
    #[getter]
    fn min_well_vol(&self) -> Option<f64> {
        self.inner.min_well_vol
    }
    #[getter]
    fn max_well_vol(&self) -> Option<f64> {
        self.inner.max_well_vol
    }
    #[getter]
    fn max_vol_total(&self) -> Option<f64> {
        self.inner.max_vol_total
    }
    #[getter]
    fn min_volume(&self) -> Option<f64> {
        self.inner.min_volume
    }
    #[getter]
    fn drop_volume(&self) -> Option<f64> {
        self.inner.drop_volume
    }

    #[getter]
    fn shape(&self) -> (u32, u32) {
        self.inner.shape()
    }

    fn __repr__(&self) -> String {
        format!(
            "PlateInfo(plate_type={:?}, usage={:?}, rows={}, cols={})",
            self.inner.plate_type, self.inner.usage, self.inner.rows, self.inner.cols
        )
    }
}

impl From<PlateInfo> for PyPlateInfo {
    fn from(p: PlateInfo) -> Self {
        Self { inner: p }
    }
}

#[pyclass(name = "Labware", module = "kithairon._native")]
struct PyLabware {
    inner: Labware,
}

#[pymethods]
impl PyLabware {
    #[new]
    fn new(plates: Option<Vec<PyPlateInfo>>) -> Self {
        let plates = plates
            .unwrap_or_default()
            .into_iter()
            .map(|p| p.inner)
            .collect();
        Self {
            inner: Labware::new(plates),
        }
    }

    #[staticmethod]
    fn from_xml_str(xml: &str) -> PyResult<Self> {
        Labware::from_xml_str(xml)
            .map(|inner| Self { inner })
            .map_err(map_err)
    }

    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        Labware::from_file(path)
            .map(|inner| Self { inner })
            .map_err(map_err)
    }

    fn to_elwx_string(&self) -> PyResult<String> {
        self.inner.to_elwx_string().map_err(map_err)
    }

    fn to_file(&self, path: &str) -> PyResult<()> {
        self.inner.to_file(path).map_err(map_err)
    }

    fn keys(&self) -> Vec<String> {
        self.inner.keys().into_iter().map(|s| s.to_string()).collect()
    }

    fn plates(&self) -> Vec<PyPlateInfo> {
        self.inner.plates().iter().cloned().map(PyPlateInfo::from).collect()
    }

    fn add(&mut self, plate: PyPlateInfo) -> PyResult<()> {
        self.inner.add(plate.inner).map_err(map_err)
    }

    fn __getitem__(&self, plate_type: &str) -> PyResult<PyPlateInfo> {
        self.inner
            .get(plate_type)
            .cloned()
            .map(PyPlateInfo::from)
            .ok_or_else(|| PyIndexError::new_err(plate_type.to_string()))
    }

    fn __len__(&self) -> usize {
        self.inner.plates().len()
    }

    fn __contains__(&self, plate_type: &str) -> bool {
        self.inner.get(plate_type).is_some()
    }

    fn __repr__(&self) -> String {
        format!("Labware({} plate types)", self.inner.plates().len())
    }
}

fn map_err(e: crate::LibraryError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

// ---------------------------------------------------------------------------
// Survey bindings.
//
// We hand survey data to Python as a list of per-well dicts, with the
// survey-level metadata duplicated on every row. Python then feeds the list
// into `pl.DataFrame(records)` to build the canonical SurveyData frame.
// This is the same shape the prior pydantic-xml code produced.

fn feature_to_dict<'py>(py: Python<'py>, f: &SignalFeature) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("feature_type", &f.feature_type)?;
    d.set_item("tof", f.tof)?;
    d.set_item("vpp", f.vpp)?;
    Ok(d)
}

fn signal_to_dict<'py>(py: Python<'py>, e: &EchoSignal) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("signal_type", &e.signal_type)?;
    d.set_item("transducer_x", e.transducer_x)?;
    d.set_item("transducer_y", e.transducer_y)?;
    d.set_item("transducer_z", e.transducer_z)?;
    let features = PyList::empty(py);
    for f in &e.features {
        features.append(feature_to_dict(py, f)?)?;
    }
    d.set_item("features", features)?;
    Ok(d)
}

fn well_to_dict<'py>(
    py: Python<'py>,
    w: &WellSurvey,
    survey: &PlateSurvey,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    // Well-level columns.
    d.set_item("row", w.row)?;
    d.set_item("column", w.column)?;
    d.set_item("well", &w.well)?;
    d.set_item("volume", w.volume)?;
    d.set_item("current_volume", w.current_volume)?;
    d.set_item("status", &w.status)?;
    d.set_item("fluid", &w.fluid)?;
    d.set_item("fluid_units", &w.fluid_units)?;
    d.set_item("meniscus_x", w.meniscus_x)?;
    d.set_item("meniscus_y", w.meniscus_y)?;
    d.set_item("fluid_composition", w.fluid_composition)?;
    d.set_item("dmso_homogeneous", w.dmso_homogeneous)?;
    d.set_item("dmso_inhomogeneous", w.dmso_inhomogeneous)?;
    d.set_item("fluid_thickness", w.fluid_thickness)?;
    d.set_item("current_fluid_thickness", w.current_fluid_thickness)?;
    d.set_item("bottom_thickness", w.bottom_thickness)?;
    d.set_item("fluid_thickness_homogeneous", w.fluid_thickness_homogeneous)?;
    d.set_item(
        "fluid_thickness_imhomogeneous",
        w.fluid_thickness_imhomogeneous,
    )?;
    d.set_item("outlier", w.outlier)?;
    d.set_item("corrective_action", &w.corrective_action)?;
    d.set_item(
        "echo_signal",
        match &w.echo_signal {
            Some(s) => signal_to_dict(py, s)?.into_any(),
            None => py.None().into_bound(py),
        },
    )?;
    // Survey-level metadata, duplicated per row so the resulting DataFrame
    // has constant columns.
    d.set_item("plate_type", &survey.plate_type)?;
    d.set_item("plate_barcode", &survey.plate_barcode)?;
    d.set_item("timestamp", survey.timestamp)?;
    d.set_item("instrument_serial_number", &survey.instrument_serial_number)?;
    d.set_item("vtl", survey.vtl)?;
    d.set_item("original", survey.original)?;
    d.set_item("data_format_version", survey.data_format_version)?;
    d.set_item("survey_rows", survey.survey_rows)?;
    d.set_item("survey_columns", survey.survey_columns)?;
    d.set_item("survey_total_wells", survey.survey_total_wells)?;
    d.set_item("plate_name", &survey.plate_name)?;
    d.set_item("comment", &survey.comment)?;
    Ok(d)
}

fn survey_to_records<'py>(py: Python<'py>, ps: &PlateSurvey) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for w in &ps.wells {
        list.append(well_to_dict(py, w, ps)?)?;
    }
    Ok(list)
}

/// Parse a survey XML file (auto-detecting platesurvey vs surveyreport) and
/// return per-well records as a list of dicts.
#[pyfunction]
fn read_survey_file_records<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyList>> {
    let ps = read_survey_file(path).map_err(map_err)?;
    survey_to_records(py, &ps)
}

/// Parse a survey XML string (auto-detecting platesurvey vs surveyreport) and
/// return per-well records as a list of dicts.
#[pyfunction]
fn read_survey_str_records<'py>(py: Python<'py>, xml: &str) -> PyResult<Bound<'py, PyList>> {
    let ps = read_survey_str(xml).map_err(map_err)?;
    survey_to_records(py, &ps)
}

#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPlateInfo>()?;
    m.add_class::<PyLabware>()?;
    m.add_function(wrap_pyfunction!(read_survey_file_records, m)?)?;
    m.add_function(wrap_pyfunction!(read_survey_str_records, m)?)?;
    Ok(())
}

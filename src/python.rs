//! PyO3 bindings exposing the Rust API to Python as `kithairon._native`.

use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;

use crate::labware::{Labware, PlateInfo};

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

#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPlateInfo>()?;
    m.add_class::<PyLabware>()?;
    Ok(())
}

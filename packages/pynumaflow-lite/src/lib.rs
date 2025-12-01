pub mod accumulate;
pub mod batchmap;
pub mod map;
pub mod mapstream;
pub mod pyiterables;
pub mod pyrs;
pub mod reduce;
pub mod reducestream;
pub mod session_reduce;
pub mod sink;
pub mod source;

use pyo3::prelude::*;

/// Submodule: pynumaflow_lite.mapper
#[pymodule]
fn mapper(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::map::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.batchmapper
#[pymodule]
fn batchmapper(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::batchmap::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.mapstreamer
#[pymodule]
fn mapstreamer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::mapstream::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.reducer
#[pymodule]
fn reducer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::reduce::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.session_reducer
#[pymodule]
fn session_reducer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::session_reduce::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.reducestreamer
#[pymodule]
fn reducestreamer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::reducestream::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.accumulator
#[pymodule]
fn accumulator(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::accumulate::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.sinker
#[pymodule]
fn sinker(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::sink::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.sourcer
#[pymodule]
fn sourcer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::source::populate_py_module(m)?;
    Ok(())
}

/// Top-level Python module `pynumaflow_lite` with submodules like `mapper`, `batchmapper`, and `mapstreamer`.
#[pymodule]
fn pynumaflow_lite(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Register the submodules via wrap_pymodule!
    m.add_wrapped(pyo3::wrap_pymodule!(mapper))?;
    m.add_wrapped(pyo3::wrap_pymodule!(batchmapper))?;
    m.add_wrapped(pyo3::wrap_pymodule!(mapstreamer))?;
    m.add_wrapped(pyo3::wrap_pymodule!(reducer))?;
    m.add_wrapped(pyo3::wrap_pymodule!(session_reducer))?;
    m.add_wrapped(pyo3::wrap_pymodule!(reducestreamer))?;
    m.add_wrapped(pyo3::wrap_pymodule!(accumulator))?;
    m.add_wrapped(pyo3::wrap_pymodule!(sinker))?;
    m.add_wrapped(pyo3::wrap_pymodule!(sourcer))?;

    // Ensure it's importable as `pynumaflow_lite.mapper` as well as attribute access
    let binding = m.getattr("mapper")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.mapper";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.batchmapper` as well
    let binding = m.getattr("batchmapper")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.batchmapper";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.mapstreamer` as well
    let binding = m.getattr("mapstreamer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.mapstreamer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.reducer` as well
    let binding = m.getattr("reducer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.reducer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.session_reducer` as well
    let binding = m.getattr("session_reducer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.session_reducer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.reducestreamer` as well
    let binding = m.getattr("reducestreamer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.reducestreamer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.accumulator` as well
    let binding = m.getattr("accumulator")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.accumulator";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.sinker` as well
    let binding = m.getattr("sinker")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.sinker";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    // Ensure it's importable as `pynumaflow_lite.sourcer` as well
    let binding = m.getattr("sourcer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow_lite.sourcer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, &sub)?;

    Ok(())
}

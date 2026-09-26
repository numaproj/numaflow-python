pub mod accumulate;
pub mod batchmap;
pub mod map;
pub mod mapstream;
pub mod nack;
pub mod pyiterables;
pub mod pyrs;
pub mod reduce;
pub mod reducestream;
pub mod session_reduce;
pub mod sideinput;
pub mod sink;
pub mod source;
pub mod sourcetransform;

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

/// Submodule: pynumaflow_lite.sourcetransformer
#[pymodule]
fn sourcetransformer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::sourcetransform::populate_py_module(m)?;
    Ok(())
}

/// Submodule: pynumaflow_lite.sideinputer
#[pymodule]
fn sideinputer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::sideinput::populate_py_module(m)?;
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
    m.add_wrapped(pyo3::wrap_pymodule!(sourcetransformer))?;
    m.add_wrapped(pyo3::wrap_pymodule!(sideinputer))?;

    // Ensure each submodule is importable as `pynumaflow_lite.<name>` as well as attribute access
    let sys_modules = py.import("sys")?.getattr("modules")?;
    for name in [
        "mapper",
        "batchmapper",
        "mapstreamer",
        "reducer",
        "session_reducer",
        "reducestreamer",
        "accumulator",
        "sinker",
        "sourcer",
        "sourcetransformer",
        "sideinputer",
    ] {
        let binding = m.getattr(name)?;
        let sub = binding.cast::<PyModule>()?;
        let fullname = format!("pynumaflow_lite.{name}");
        sub.setattr("__name__", &fullname)?;
        sys_modules.set_item(&fullname, sub)?;
    }

    Ok(())
}

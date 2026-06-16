//! Minimal Rust extension for pynumaflow, exposing only the sourcetransformer
//! submodule for now. Compiled and shipped as `pynumaflow._pynumaflow_rs`.
//!
//! Additional UDF types will be migrated here incrementally.

pub mod pyrs;
pub mod sourcetransform;

use pyo3::prelude::*;

/// Submodule: pynumaflow._pynumaflow_rs.sourcetransformer
#[pymodule]
fn sourcetransformer(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    crate::sourcetransform::populate_py_module(m)?;
    Ok(())
}

/// Top-level compiled module `pynumaflow._pynumaflow_rs`.
#[pymodule]
#[pyo3(name = "_pynumaflow_rs")]
fn pynumaflow_rs(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(pyo3::wrap_pymodule!(sourcetransformer))?;

    // Make it importable as `pynumaflow._pynumaflow_rs.sourcetransformer`
    // (not just attribute access on the parent module).
    let binding = m.getattr("sourcetransformer")?;
    let sub = binding.cast::<PyModule>()?;
    let fullname = "pynumaflow._pynumaflow_rs.sourcetransformer";
    sub.setattr("__name__", fullname)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(fullname, sub)?;

    Ok(())
}

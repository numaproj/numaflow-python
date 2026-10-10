use std::collections::HashMap;

use pyo3::prelude::*;

use crate::pyrs::py_repr;

/// Per-message redelivery options for a nack.
///
/// Shared across all UDF types (mapper, batchmapper, mapstreamer,
/// sourcetransformer, sinker, sourcer). It mirrors `numaflow::shared::NackOptions`.
#[pyclass(module = "pynumaflow_lite", name = "NackOptions", from_py_object, eq)]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NackOptions {
    /// Redelivery delay in milliseconds.
    #[pyo3(get)]
    pub delay: Option<u64>,
    /// Maximum number of redelivery attempts.
    #[pyo3(get)]
    pub max_deliveries: Option<u32>,
    /// Human-readable reason for nacking the message.
    #[pyo3(get)]
    pub reason: Option<String>,
    /// Generic key-value options passed back to the source on nack.
    #[pyo3(get)]
    pub nack_map: HashMap<String, String>,
}

#[pymethods]
impl NackOptions {
    #[new]
    #[pyo3(signature = (delay: "int | None"=None, max_deliveries: "int | None"=None, reason: "str | None"=None, nack_map: "dict[str, str] | None"=None) -> "NackOptions")]
    fn new(
        delay: Option<u64>,
        max_deliveries: Option<u32>,
        reason: Option<String>,
        nack_map: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            delay,
            max_deliveries,
            reason,
            nack_map: nack_map.unwrap_or_default(),
        }
    }

    pub(crate) fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "NackOptions(delay={}, max_deliveries={}, reason={}, nack_map={})",
            py_repr(py, self.delay)?,
            py_repr(py, self.max_deliveries)?,
            py_repr(py, &self.reason)?,
            py_repr(py, &self.nack_map)?,
        ))
    }
}

impl From<numaflow::shared::NackOptions> for NackOptions {
    fn from(value: numaflow::shared::NackOptions) -> Self {
        Self {
            delay: value.delay,
            max_deliveries: value.max_deliveries,
            reason: value.reason,
            nack_map: value.nack_map,
        }
    }
}

impl From<NackOptions> for numaflow::shared::NackOptions {
    fn from(value: NackOptions) -> Self {
        Self {
            delay: value.delay,
            max_deliveries: value.max_deliveries,
            reason: value.reason,
            nack_map: value.nack_map,
        }
    }
}

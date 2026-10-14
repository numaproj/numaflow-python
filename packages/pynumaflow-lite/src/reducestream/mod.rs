use std::sync::Mutex;

use numaflow::reducestream;

pub mod server;

use pyo3::prelude::*;

// Re-export types from reduce module that are shared
pub use crate::reduce::{Datum, IntervalWindow, Message, Metadata, PyAsyncDatumStream};

/// Async Reduce Stream Server that can be started from Python code, taking a reducer factory.
#[pyclass(
    name = "_ReduceStreamAsyncServer",
    module = "pynumaflow_lite.reducestreamer"
)]
pub struct ReduceStreamAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl ReduceStreamAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_ReduceStreamAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| reducestream::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| reducestream::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given zero-argument factory. The server calls the
    /// factory once for each window and key set to get the async generator handler.
    #[pyo3(signature = (factory: "callable") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, factory: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let server_info_file = self.server_info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(
            py,
            crate::reducestream::server::start(factory, sock_file, server_info_file, rx),
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the reducestream UDS.
    #[pyo3(signature = () -> "None")]
    pub fn wait_ready<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::reduce::wait_for_ready(sock_file).await;
            Ok(())
        })
    }

    /// Trigger server shutdown from Python (idempotent).
    #[pyo3(signature = () -> "None")]
    pub fn stop(&self) -> PyResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Helper to populate a PyModule with reduce stream types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<IntervalWindow>()?;
    m.add_class::<Metadata>()?;
    m.add_class::<ReduceStreamAsyncServer>()?;
    Ok(())
}

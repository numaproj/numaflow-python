use std::sync::Mutex;

pub mod server;

use pyo3::prelude::*;

// Re-export types from reduce module that are shared
pub use crate::reduce::{Datum, IntervalWindow, Message, Metadata, PyAsyncDatumStream};

/// Async Reduce Stream Server that can be started from Python code, taking a class (creator).
#[pyclass(module = "pynumaflow_lite.reducestreamer")]
pub struct ReduceStreamAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl ReduceStreamAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file="/var/run/numaflow/reducestream.sock".to_string(), info_file="/var/run/numaflow/reducestreamer-server-info".to_string()))]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python class (creator).
    /// Only class-based reducers are supported (not function-based).
    /// init_args is an optional tuple of positional arguments to pass to the class constructor.
    #[pyo3(signature = (py_creator, init_args=None))]
    pub fn start<'a>(
        &self,
        py: Python<'a>,
        py_creator: Py<PyAny>,
        init_args: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let info_file = self.info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::reducestream::server::start(py_creator, init_args, sock_file, info_file, rx)
                .await
                .expect("server failed to start");
            Ok(())
        })
    }

    /// Trigger server shutdown from Python (idempotent).
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
    m.add_class::<PyAsyncDatumStream>()?;
    Ok(())
}

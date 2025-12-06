/// Side Input interface managed by Python. It means Python code will start the server
/// and can pass in the Python class implementing the SideInput interface.
pub mod server;

use pyo3::prelude::*;
use std::sync::Mutex;

/// Response from the side input retrieve handler.
/// Indicates whether to broadcast a value or not.
#[pyclass(module = "pynumaflow_lite.sideinputer")]
#[derive(Clone, Debug)]
pub struct Response {
    /// The value to broadcast (if any).
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// Whether to broadcast this value.
    #[pyo3(get)]
    pub broadcast: bool,
}

#[pymethods]
impl Response {
    /// Create a new Response that broadcasts the given value.
    #[staticmethod]
    #[pyo3(signature = (value))]
    fn broadcast_message(value: Vec<u8>) -> Self {
        Self {
            value,
            broadcast: true,
        }
    }

    /// Create a new Response that does not broadcast any value.
    #[staticmethod]
    #[pyo3(signature = ())]
    fn no_broadcast_message() -> Self {
        Self {
            value: vec![],
            broadcast: false,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Response(value={:?}, broadcast={})",
            self.value, self.broadcast
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Async SideInput Server that can be started from Python code which will run the Python UDF.
#[pyclass(module = "pynumaflow_lite.sideinputer")]
pub struct SideInputAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SideInputAsyncServer {
    /// Create a new SideInputAsyncServer.
    ///
    /// Args:
    ///     sock_file: Path to the Unix domain socket file.
    ///     info_file: Path to the server info file.
    #[new]
    #[pyo3(signature = (sock_file=None, info_file=None))]
    fn new(sock_file: Option<String>, info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| "/var/run/numaflow/sideinput.sock".to_string()),
            info_file: info_file
                .unwrap_or_else(|| "/var/run/numaflow/sideinput-server-info".to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python SideInput class instance.
    #[pyo3(signature = (py_sideinput: "SideInput") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, py_sideinput: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let info_file = self.info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::sideinput::server::start(py_sideinput, sock_file, info_file, rx)
                .await
                .expect("server failed to start");
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

/// Helper to populate a PyModule with sideinput types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Response>()?;
    m.add_class::<SideInputAsyncServer>()?;

    Ok(())
}

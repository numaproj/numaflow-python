use chrono::{DateTime, Utc};
use numaflow::reduce;
use std::collections::HashMap;
use std::sync::Mutex;

pub mod server;

use pyo3::prelude::*;
use tokio::sync::mpsc;

/// A message to be sent to the next vertex from a reduce handler.
#[pyclass(module = "pynumaflow_lite.reducer")]
#[derive(Clone, Default, Debug)]
pub struct Message {
    pub keys: Option<Vec<String>>, // optional keys
    pub value: Vec<u8>,            // payload
    pub tags: Option<Vec<String>>, // optional tags (e.g., DROP)
}

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None) -> "Message")]
    fn new(value: Vec<u8>, keys: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        Self { keys, value, tags }
    }

    /// Drop a Message, do not forward to the next vertex.
    #[pyo3(signature = ())]
    #[staticmethod]
    fn message_to_drop() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
        }
    }
}

impl From<Message> for reduce::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
        }
    }
}

/// A collection of Messages returned by reducer.
#[pyclass(module = "pynumaflow_lite.reducer")]
#[derive(Clone, Debug)]
pub struct Messages {
    pub(crate) messages: Vec<Message>,
}

#[pymethods]
impl Messages {
    #[new]
    #[pyo3(signature = () -> "Messages")]
    fn new() -> Self {
        Self { messages: vec![] }
    }

    /// Append a Message to the collection.
    #[pyo3(signature = (message: "Message"))]
    fn append(&mut self, message: Message) {
        self.messages.push(message);
    }

    fn __repr__(&self) -> String {
        format!("Messages({:?})", self.messages)
    }

    fn __str__(&self) -> String {
        format!("Messages({:?})", self.messages)
    }
}

/// The incoming ReduceRequest accessible in Python function (streamed).
#[pyclass(module = "pynumaflow_lite.reducer")]
pub struct Datum {
    #[pyo3(get)]
    pub keys: Vec<String>,
    #[pyo3(get)]
    pub value: Vec<u8>,
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    #[pyo3(get)]
    pub eventtime: DateTime<Utc>,
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
}

impl Datum {
    fn new(
        keys: Vec<String>,
        value: Vec<u8>,
        watermark: DateTime<Utc>,
        eventtime: DateTime<Utc>,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            keys,
            value,
            watermark,
            eventtime,
            headers,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, headers={:?})",
            self.keys, self.value, self.watermark, self.eventtime, self.headers
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, headers={:?})",
            self.keys,
            String::from_utf8_lossy(&self.value),
            self.watermark,
            self.eventtime,
            self.headers
        )
    }
}

impl From<reduce::ReduceRequest> for Datum {
    fn from(value: reduce::ReduceRequest) -> Self {
        Self::new(
            value.keys,
            value.value,
            value.watermark,
            value.eventtime,
            value.headers,
        )
    }
}

/// IntervalWindow exposed to Python metadata
#[pyclass(module = "pynumaflow_lite.reducer")]
#[derive(Clone)]
pub struct IntervalWindow {
    #[pyo3(get)]
    pub start: DateTime<Utc>,
    #[pyo3(get)]
    pub end: DateTime<Utc>,
}

impl IntervalWindow {
    pub(crate) fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }
}

/// Metadata passed to reducer handler in Python.
#[pyclass(module = "pynumaflow_lite.reducer")]
#[derive(Clone)]
pub struct Metadata {
    #[pyo3(get)]
    pub interval_window: IntervalWindow,
}

impl Metadata {
    pub(crate) fn new(interval_window: IntervalWindow) -> Self {
        Self { interval_window }
    }
}

/// Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
/// This is a thin wrapper around the generic AsyncChannelStream implementation.
#[pyclass(module = "pynumaflow_lite.reducer")]
pub struct PyAsyncDatumStream {
    inner: crate::pyiterables::AsyncChannelStream<Datum>,
}

#[pymethods]
impl PyAsyncDatumStream {
    #[new]
    fn new() -> Self {
        let (_tx, rx) = mpsc::channel::<Datum>(1);
        Self {
            inner: crate::pyiterables::AsyncChannelStream::new(rx),
        }
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.inner.py_anext(py)
    }
}

impl PyAsyncDatumStream {
    pub fn new_with(rx: mpsc::Receiver<Datum>) -> Self {
        Self {
            inner: crate::pyiterables::AsyncChannelStream::new(rx),
        }
    }
}

/// Async Reduce Server that can be started from Python code, taking a class (creator).
#[pyclass(module = "pynumaflow_lite.reducer")]
pub struct ReduceAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl ReduceAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=reduce::SOCK_ADDR.to_string(), info_file: "str | None"=reduce::SERVER_INFO_FILE.to_string()) -> "ReduceAsyncServer")]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python class (creator) or function.
    /// - For class-based: pass the class and optionally init_args (tuple).
    /// - For function-based: pass the async function directly (init_args is ignored).
    #[pyo3(signature = (py_creator: "object", init_args: "object | None"=None) -> "None")]
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
            crate::reduce::server::start(py_creator, init_args, sock_file, info_file, rx)
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

/// Helper to populate a PyModule with reduce types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Messages>()?;
    m.add_class::<Datum>()?;
    m.add_class::<IntervalWindow>()?;
    m.add_class::<Metadata>()?;
    m.add_class::<ReduceAsyncServer>()?;
    m.add_class::<PyAsyncDatumStream>()?;
    Ok(())
}

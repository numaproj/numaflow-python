use std::collections::HashMap;

use numaflow::batchmap;

use chrono::{DateTime, Utc};

/// BatchMap interface managed by Python. Python code will start the server
/// and can pass in the Python coroutine.
pub mod server;

use tokio::sync::mpsc;

use pyo3::prelude::*;
use std::sync::Mutex;

/// A message to be sent for a single datum in batch response.
#[pyclass(module = "pynumaflow_lite.batchmapper")]
#[derive(Clone, Default, Debug)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for conditional forwarding.
    pub tags: Option<Vec<String>>,
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given value, keys, and tags.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None) -> "Message"
    )]
    fn new(value: Vec<u8>, keys: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        Self { keys, value, tags }
    }

    /// Drop a [Message], do not forward to the next vertex.
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

impl From<Message> for batchmap::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
        }
    }
}

/// The incoming Datum for BatchMap
#[pyclass(module = "pynumaflow_lite.batchmapper")]
pub struct Datum {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// watermark represented by time is a guarantee that we will not see an element older than this time.
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    #[pyo3(get)]
    pub eventtime: DateTime<Utc>,
    /// ID is the unique id of the message
    #[pyo3(get)]
    pub id: String,
    /// Headers for the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
}

impl Datum {
    fn new(
        keys: Vec<String>,
        value: Vec<u8>,
        watermark: DateTime<Utc>,
        eventtime: DateTime<Utc>,
        id: String,
        headers: HashMap<String, String>,
    ) -> Self {
        Self {
            keys,
            value,
            watermark,
            eventtime,
            id,
            headers,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, id={}, headers={:?})",
            self.keys, self.value, self.watermark, self.eventtime, self.id, self.headers
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, id={}, headers={:?})",
            self.keys,
            String::from_utf8_lossy(&self.value),
            self.watermark,
            self.eventtime,
            self.id,
            self.headers
        )
    }
}

impl From<batchmap::Datum> for Datum {
    fn from(value: batchmap::Datum) -> Self {
        Datum::new(
            value.keys,
            value.value,
            value.watermark,
            value.event_time,
            value.id,
            value.headers,
        )
    }
}

/// BatchResponse mirrors numaflow::batchmap::BatchResponse for Python
#[pyclass(module = "pynumaflow_lite.batchmapper")]
#[derive(Clone, Debug)]
pub struct BatchResponse {
    #[pyo3(get)]
    pub id: String,
    pub messages: Vec<Message>,
}

#[pymethods]
impl BatchResponse {
    #[new]
    #[pyo3(signature = (id: "str") -> "BatchResponse")]
    fn new(id: String) -> Self {
        Self {
            id,
            messages: Vec::new(),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (id: "str") -> "BatchResponse")]
    fn from_id(id: String) -> Self {
        Self {
            id,
            messages: Vec::new(),
        }
    }

    #[pyo3(signature = (message))]
    fn append(&mut self, message: Message) {
        self.messages.push(message);
    }
}

/// A collection of BatchResponse objects for a batch.
#[pyclass(module = "pynumaflow_lite.batchmapper")]
#[derive(Clone, Debug)]
pub struct BatchResponses {
    pub(crate) responses: Vec<BatchResponse>,
}

#[pymethods]
impl BatchResponses {
    #[new]
    #[pyo3(signature = () -> "BatchResponses")]
    fn new() -> Self {
        Self { responses: vec![] }
    }

    /// Append a BatchResponse to the collection.
    #[pyo3(signature = (response: "BatchResponse"))]
    fn append(&mut self, response: BatchResponse) {
        self.responses.push(response);
    }
}

impl From<BatchResponse> for batchmap::BatchResponse {
    fn from(value: BatchResponse) -> Self {
        let mut resp = batchmap::BatchResponse::from_id(value.id);
        for m in value.messages.into_iter() {
            resp.append(m.into());
        }
        resp
    }
}

/// Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
/// This is a thin wrapper around the generic AsyncChannelStream implementation.
#[pyclass(module = "pynumaflow_lite.batchmapper")]
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

/// Async Batch Map Server that can be started from Python code
#[pyclass(module = "pynumaflow_lite.batchmapper")]
pub struct BatchMapAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl BatchMapAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=batchmap::SOCK_ADDR.to_string(), info_file: "str | None"=batchmap::SERVER_INFO_FILE.to_string()) -> "BatchMapAsyncServer"
    )]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    #[pyo3(signature = (py_func: "callable") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, py_func: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let info_file = self.info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // batch server uses the same runner loop and shutdown composition for now
            // dedicated start is wired below
            crate::batchmap::server::start(py_func, sock_file, info_file, rx)
                .await
                .expect("server failed to start");
            Ok(())
        })
    }

    #[pyo3(signature = () -> "None")]
    pub fn stop(&self) -> PyResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Helper to populate a PyModule with batch map types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<BatchResponse>()?;
    m.add_class::<BatchResponses>()?;
    m.add_class::<BatchMapAsyncServer>()?;
    m.add_class::<PyAsyncDatumStream>()?;

    Ok(())
}

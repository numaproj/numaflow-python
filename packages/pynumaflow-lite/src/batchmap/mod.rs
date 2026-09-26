use std::{collections::HashMap, time::Duration};

use numaflow::batchmap;

use chrono::{DateTime, Utc};

/// BatchMap interface managed by Python. Python code will start the server
/// and can pass in the Python coroutine.
pub mod server;

use tokio::sync::mpsc;

use pyo3::prelude::*;
use std::sync::Mutex;

use crate::{nack::NackOptions, pyrs::bytes_literal};

/// A message to be sent for a single datum in batch response.
#[pyclass(module = "pynumaflow_lite.batchmapper", from_py_object, eq)]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    #[pyo3(get)]
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// Tags are used for conditional forwarding.
    #[pyo3(get)]
    pub tags: Option<Vec<String>>,
    /// Options sent back to the source when nacking the message.
    #[pyo3(get)]
    pub nack_options: Option<NackOptions>,
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given value. The keys and tags are optional.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None) -> "Message"
    )]
    fn new(value: Vec<u8>, keys: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        Self {
            keys,
            value,
            tags,
            nack_options: None,
        }
    }

    /// Drop a [Message], do not forward to the next vertex.
    #[pyo3(signature = () -> "Message")]
    #[staticmethod]
    fn to_drop() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
            nack_options: None,
        }
    }

    /// A Message marked to be negatively acknowledged (retried), with optional nack options.
    #[staticmethod]
    #[pyo3(signature = (nack_options: "NackOptions | None"=None) -> "Message")]
    fn to_nack(nack_options: Option<NackOptions>) -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::NACK.to_string()]),
            nack_options,
        }
    }

    /// A Message marked to be failed.
    #[staticmethod]
    #[pyo3(signature = () -> "Message")]
    fn to_fail() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::FAIL.to_string()]),
            nack_options: None,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(value=b\"{:?}\", keys={}, tags={}, nack_options={})",
            &self.value,
            self.keys
                .as_ref()
                .map_or_else(|| "None".to_string(), |keys| format!("{keys:?}")),
            self.tags
                .as_ref()
                .map_or_else(|| "None".to_string(), |tags| format!("{tags:?}")),
            self.nack_options.as_ref().map_or_else(
                || "None".to_string(),
                |nack_options| nack_options.__repr__()
            ),
        )
    }
}

impl From<Message> for batchmap::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
            nack_options: value.nack_options.map(Into::into),
        }
    }
}

/// The incoming Datum passed to the BatchMap handler.
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
    pub event_time: DateTime<Utc>,
    /// ID is the unique id of the message
    #[pyo3(get)]
    pub id: String,
    /// Headers for the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
}

#[pymethods]
impl Datum {
    #[new]
    #[pyo3(signature = (
        *,
        id: "str",
        keys: "list[str] | None"=None,
        value: "bytes | None"=None,
        event_time: "datetime.datetime | None"=None,
        watermark: "datetime.datetime | None"=None,
        headers: "dict[str, str] | None"=None,
    ) -> "Datum")]
    fn new(
        id: String,
        keys: Option<Vec<String>>,
        value: Option<Vec<u8>>,
        event_time: Option<DateTime<Utc>>,
        watermark: Option<DateTime<Utc>>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            id,
            keys: keys.unwrap_or_default(),
            value: value.unwrap_or_default(),
            watermark: watermark.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            event_time: event_time.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            headers: headers.unwrap_or_default(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(id={}, keys={:?}, value={}, watermark={}, event_time={}, headers={:?})",
            self.id, self.keys, bytes_literal(&self.value), self.watermark, self.event_time, self.headers
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<batchmap::Datum> for Datum {
    fn from(value: batchmap::Datum) -> Self {
        Datum::new(
            value.id,
            Some(value.keys),
            Some(value.value),
            Some(value.watermark),
            Some(value.event_time),
            Some(value.headers),
        )
    }
}

/// BatchResponse mirrors numaflow::batchmap::BatchResponse for Python
#[pyclass(module = "pynumaflow_lite.batchmapper", from_py_object)]
#[derive(Clone, Debug)]
pub struct BatchResponse {
    #[pyo3(get)]
    pub id: String,
    pub messages: Vec<Message>,
}

#[pymethods]
impl BatchResponse {
    #[new]
    /// Create a new [BatchResponse] for the given datum id, with optional messages.
    #[pyo3(signature = (id: "str", *messages: "Message") -> "BatchResponse")]
    fn new(id: String, messages: Vec<Message>) -> Self {
        Self { id, messages }
    }

    #[pyo3(signature = (message))]
    fn append(&mut self, message: Message) {
        self.messages.push(message);
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
#[pyclass(name = "_BatchMapAsyncServer", module = "pynumaflow_lite.batchmapper")]
pub struct BatchMapAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl BatchMapAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str"=batchmap::SOCK_ADDR.to_string(),
        server_info_file: "str"=batchmap::SERVER_INFO_FILE.to_string()
    ) -> "_BatchMapAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| batchmap::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| batchmap::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    #[pyo3(signature = (handler: "callable") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, handler: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let info_file = self.server_info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(
            py,
            crate::batchmap::server::start(handler, sock_file, info_file, rx)
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the map UDS.
    #[pyo3(signature = (timeout: "float"=30.0) -> "None")]
    pub fn wait_ready<'a>(&self, py: Python<'a>, timeout: f64) -> PyResult<Bound<'a, PyAny>> {
        if !timeout.is_finite() || timeout < 0.0 {
            return Err(pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "timeout must be a non-negative finite float",
            ));
        }

        let sock_file = self.sock_file.clone();
        let timeout = Duration::from_secs_f64(timeout);

        pyo3_async_runtimes::tokio::future_into_py(
            py,
            crate::map::wait_for_ready(sock_file, timeout, "map")
        )
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
    m.add_class::<NackOptions>()?;
    m.add_class::<BatchResponse>()?;
    m.add_class::<BatchMapAsyncServer>()?;
    m.add_class::<PyAsyncDatumStream>()?;

    Ok(())
}

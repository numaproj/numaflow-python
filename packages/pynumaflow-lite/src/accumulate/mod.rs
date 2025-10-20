use chrono::{DateTime, Utc};
use numaflow::accumulator;
use std::collections::HashMap;
use std::sync::Mutex;

pub mod server;

use pyo3::prelude::*;
use tokio::sync::mpsc;

/// A message to be sent to the next vertex from an accumulator handler.
#[pyclass(module = "pynumaflow_lite.accumulator")]
#[derive(Clone, Debug)]
pub struct Message {
    #[pyo3(get)]
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Option<Vec<String>>,
    #[pyo3(get)]
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    #[pyo3(get)]
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Option<Vec<String>>,
    #[pyo3(get)]
    /// ID is used for deduplication. Read-only, set from the input datum.
    pub id: String,
    #[pyo3(get)]
    /// Headers for the message. Read-only, set from the input datum.
    pub headers: HashMap<String, String>,
    #[pyo3(get)]
    /// Time of the element as seen at source or aligned after a reduce operation. Read-only, set from the input datum.
    pub event_time: DateTime<Utc>,
    #[pyo3(get)]
    /// Watermark represented by time is a guarantee that we will not see an element older than this time. Read-only, set from the input datum.
    pub watermark: DateTime<Utc>,
}

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (
        value,
        keys=None,
        tags=None,
        id=String::new(),
        headers=HashMap::new(),
        event_time=chrono::Utc::now(),
        watermark=chrono::Utc::now()
    ))]
    fn new(
        value: Vec<u8>,
        keys: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        id: String,
        headers: HashMap<String, String>,
        event_time: DateTime<Utc>,
        watermark: DateTime<Utc>,
    ) -> Self {
        Self {
            keys,
            value,
            tags,
            id,
            headers,
            event_time,
            watermark,
        }
    }

    /// Drop a Message, do not forward to the next vertex.
    #[pyo3(signature = ())]
    #[staticmethod]
    fn message_to_drop() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
            id: String::new(),
            headers: HashMap::new(),
            event_time: chrono::Utc::now(),
            watermark: chrono::Utc::now(),
        }
    }

    /// Create a Message from a Datum, preserving all metadata.
    #[pyo3(signature = (datum, value=None, keys=None, tags=None))]
    #[staticmethod]
    fn from_datum(
        datum: &Datum,
        value: Option<Vec<u8>>,
        keys: Option<Vec<String>>,
        tags: Option<Vec<String>>,
    ) -> Self {
        Self {
            keys: keys.or_else(|| Some(datum.keys.clone())),
            value: value.unwrap_or_else(|| datum.value.clone()),
            tags,
            id: datum.id.clone(),
            headers: datum.headers.clone(),
            event_time: datum.event_time,
            watermark: datum.watermark,
        }
    }
}

impl From<Message> for accumulator::Message {
    fn from(value: Message) -> Self {
        // Create an AccumulatorRequest with all the fields from the Message
        let request = accumulator::AccumulatorRequest {
            keys: value.keys.clone().unwrap_or_default(),
            value: value.value.clone(),
            watermark: value.watermark,
            event_time: value.event_time,
            headers: value.headers.clone(),
            id: value.id.clone(),
        };

        let mut msg = Self::from_accumulator_request(request);

        // Update with optional fields
        if let Some(keys) = value.keys {
            msg = msg.with_keys(keys);
        }
        msg = msg.with_value(value.value);
        if let Some(tags) = value.tags {
            msg = msg.with_tags(tags);
        }
        msg
    }
}

/// The incoming AccumulatorRequest accessible in Python function (streamed).
#[pyclass(module = "pynumaflow_lite.accumulator")]
pub struct Datum {
    #[pyo3(get)]
    pub keys: Vec<String>,
    #[pyo3(get)]
    pub value: Vec<u8>,
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    #[pyo3(get)]
    pub event_time: DateTime<Utc>,
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
    #[pyo3(get)]
    pub id: String,
}

impl Datum {
    fn new(
        keys: Vec<String>,
        value: Vec<u8>,
        watermark: DateTime<Utc>,
        event_time: DateTime<Utc>,
        headers: HashMap<String, String>,
        id: String,
    ) -> Self {
        Self {
            keys,
            value,
            watermark,
            event_time,
            headers,
            id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, event_time={}, headers={:?}, id={:?})",
            self.keys, self.value, self.watermark, self.event_time, self.headers, self.id
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, event_time={}, headers={:?}, id={:?})",
            self.keys,
            String::from_utf8_lossy(&self.value),
            self.watermark,
            self.event_time,
            self.headers,
            self.id
        )
    }
}

impl From<accumulator::AccumulatorRequest> for Datum {
    fn from(value: accumulator::AccumulatorRequest) -> Self {
        Self::new(
            value.keys,
            value.value,
            value.watermark,
            value.event_time,
            value.headers,
            value.id,
        )
    }
}

/// Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
/// This is a thin wrapper around the generic AsyncChannelStream implementation.
#[pyclass(module = "pynumaflow_lite.accumulator")]
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

/// Async Accumulator Server that can be started from Python code, taking a class (creator).
#[pyclass(module = "pynumaflow_lite.accumulator")]
pub struct AccumulatorAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl AccumulatorAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file="/var/run/numaflow/accumulator.sock".to_string(), info_file="/var/run/numaflow/accumulator-server-info".to_string()))]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python class (creator).
    /// - For class-based: pass the class and optionally init_args (tuple).
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
            crate::accumulate::server::start(py_creator, init_args, sock_file, info_file, rx)
                .await?;
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

/// Helper to populate a PyModule with accumulator types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<AccumulatorAsyncServer>()?;
    m.add_class::<PyAsyncDatumStream>()?;
    Ok(())
}

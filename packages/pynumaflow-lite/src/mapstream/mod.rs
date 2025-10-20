use chrono::{DateTime, Utc};
use numaflow::mapstream;
use std::collections::HashMap;
use std::sync::Mutex;

pub mod server;

/// Types for streaming handler
use pyo3::prelude::*;

/// Streaming Datum mirrors MapStreamRequest for Python
#[pyclass(module = "pynumaflow_lite.mapstreamer")]
#[derive(Clone)]
pub struct Datum {
    /// Set of keys in the (key, value) terminology of the map/reduce paradigm.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of the map/reduce paradigm.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// [Watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a
    /// guarantee that we will not see an element older than this time.
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    #[pyo3(get)]
    pub eventtime: DateTime<Utc>,
    /// Headers associated with the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
}

impl Datum {
    pub(crate) fn new(
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
}

impl From<numaflow::mapstream::MapStreamRequest> for Datum {
    fn from(value: numaflow::mapstream::MapStreamRequest) -> Self {
        Self::new(
            value.keys,
            value.value,
            value.watermark,
            value.eventtime,
            value.headers,
        )
    }
}

/// A message to be sent downstream from a streaming handler.
#[pyclass(module = "pynumaflow_lite.mapstreamer")]
#[derive(Clone, Default, Debug)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for conditional forwarding.
    pub tags: Option<Vec<String>>,
}

#[pymethods]
impl Message {
    /// Create a new Message with the given value, keys, and tags.
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

    /// Convenience alias to match example usage: Message.to_drop()
    #[pyo3(signature = ())]
    #[staticmethod]
    fn to_drop() -> Self {
        Self::message_to_drop()
    }
}

impl From<Message> for mapstream::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
        }
    }
}

/// Async MapStream Server that can be started from Python code which will run the Python UDF async generator.
#[pyclass(module = "pynumaflow_lite.mapstreamer")]
pub struct MapStreamAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl MapStreamAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=mapstream::SOCK_ADDR.to_string(), info_file: "str | None"=mapstream::SERVER_INFO_FILE.to_string()) -> "MapStreamAsyncServer"
    )]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python async generator function.
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
            crate::mapstream::server::start(py_func, sock_file, info_file, rx)
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

/// Helper to populate a PyModule with mapstream types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<MapStreamAsyncServer>()?;
    Ok(())
}

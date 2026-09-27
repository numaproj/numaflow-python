use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use numaflow::mapstream;
use pyo3::prelude::*;

pub mod server;

use crate::map::wait_for_ready;
use crate::nack::NackOptions;
use crate::pyrs::bytes_literal;

/// Streaming Datum mirrors MapStreamRequest for Python
#[pyclass(module = "pynumaflow_lite.mapstreamer", from_py_object)]
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
    pub event_time: DateTime<Utc>,
    /// Headers associated with the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
}

#[pymethods]
impl Datum {
    #[new]
    #[pyo3(signature = (
        *,
        keys: "list[str] | None"=None,
        value: "bytes | None"=None,
        event_time: "datetime.datetime | None"=None,
        watermark: "datetime.datetime | None"=None,
        headers: "dict[str, str] | None"=None,
    ) -> "Datum")]
    fn new(
        keys: Option<Vec<String>>,
        value: Option<Vec<u8>>,
        event_time: Option<DateTime<Utc>>,
        watermark: Option<DateTime<Utc>>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            keys: keys.unwrap_or_default(),
            value: value.unwrap_or_default(),
            watermark: watermark.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            event_time: event_time.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            headers: headers.unwrap_or_default(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={}, watermark={}, event_time={}, headers={:?})",
            self.keys,
            bytes_literal(&self.value),
            self.watermark,
            self.event_time,
            self.headers,
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<numaflow::mapstream::MapStreamRequest> for Datum {
    fn from(value: numaflow::mapstream::MapStreamRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: value.watermark,
            event_time: value.eventtime,
            headers: value.headers,
        }
    }
}

/// A message to be sent downstream from a streaming handler.
#[pyclass(module = "pynumaflow_lite.mapstreamer", from_py_object)]
#[derive(Clone, Default, Debug)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for conditional forwarding.
    pub tags: Option<Vec<String>>,
    /// Options sent back to the source when nacking the message.
    #[pyo3(get)]
    pub nack_options: Option<NackOptions>,
}

#[pymethods]
impl Message {
    /// Create a new Message with the given value, keys, and tags.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None) -> "Message")]
    fn new(value: Vec<u8>, keys: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        Self {
            keys,
            value,
            tags,
            nack_options: None,
        }
    }

    /// Drop a Message, do not forward to the next vertex.
    #[staticmethod]
    #[pyo3(signature = () -> "Message")]
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
            "Message(value={}, keys={}, tags={}, user_metadata={})",
            bytes_literal(&self.value),
            self.keys
                .as_ref()
                .map_or_else(|| "None".to_string(), |keys| format!("{keys:?}")),
            self.tags
                .as_ref()
                .map_or_else(|| "None".to_string(), |tags| format!("{tags:?}")),
            self.nack_options
                .as_ref()
                .map_or_else(|| "None".to_string(), NackOptions::__repr__),
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<Message> for mapstream::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
            nack_options: value.nack_options.map(Into::into),
        }
    }
}

/// Async MapStream Server that can be started from Python code which will run the Python UDF async generator.
#[pyclass(name = "_MapStreamAsyncServer", module = "pynumaflow_lite.mapstreamer")]
pub struct MapStreamAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl MapStreamAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_MapStreamAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| mapstream::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| mapstream::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python async generator function.
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
            crate::mapstream::server::start(handler, sock_file, info_file, rx),
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the mapstream UDS.
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
            wait_for_ready(sock_file, timeout, "mapstream"),
        )
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
    m.add_class::<NackOptions>()?;
    m.add_class::<MapStreamAsyncServer>()?;
    Ok(())
}

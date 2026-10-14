use chrono::{DateTime, Utc};
use numaflow::proto::reduce::reduce_client::ReduceClient;
use numaflow::reduce;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

pub mod server;

use pyo3::prelude::*;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tonic::transport::Uri;
use tower::service_fn;

use crate::pyrs::{bytes_literal, py_repr};

/// A message to be sent to the next vertex from a reduce handler.
#[pyclass(module = "pynumaflow_lite.reducer", from_py_object, eq)]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    #[pyo3(get)]
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    #[pyo3(get)]
    pub tags: Option<Vec<String>>,
}

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None) -> "Message")]
    fn new(value: Vec<u8>, keys: Option<Vec<String>>, tags: Option<Vec<String>>) -> Self {
        Self { keys, value, tags }
    }

    /// A Message marked to be dropped, i.e. not forwarded to the next vertex.
    #[staticmethod]
    #[pyo3(signature = () -> "Message")]
    fn to_drop() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Message(value={}, keys={}, tags={})",
            bytes_literal(&self.value),
            py_repr(py, &self.keys)?,
            py_repr(py, &self.tags)?,
        ))
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

/// The incoming ReduceRequest accessible in Python function (streamed).
#[pyclass(module = "pynumaflow_lite.reducer")]
pub struct Datum {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a
    /// guarantee that we will not see an element older than this time.
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    #[pyo3(get)]
    pub event_time: DateTime<Utc>,
    /// Headers for the message.
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

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Datum(keys={}, value={}, watermark={}, event_time={}, headers={})",
            py_repr(py, &self.keys)?,
            bytes_literal(&self.value),
            self.watermark,
            self.event_time,
            py_repr(py, &self.headers)?,
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

impl From<reduce::ReduceRequest> for Datum {
    fn from(value: reduce::ReduceRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: value.watermark,
            event_time: value.eventtime,
            headers: value.headers,
        }
    }
}

/// IntervalWindow exposed to Python metadata
#[pyclass(module = "pynumaflow_lite.reducer", from_py_object)]
#[derive(Clone)]
pub struct IntervalWindow {
    #[pyo3(get)]
    pub start: DateTime<Utc>,
    #[pyo3(get)]
    pub end: DateTime<Utc>,
}

#[pymethods]
impl IntervalWindow {
    #[new]
    #[pyo3(signature = (start: "datetime.datetime", end: "datetime.datetime") -> "IntervalWindow")]
    pub(crate) fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }
}

/// Metadata passed to reducer handler in Python.
#[pyclass(module = "pynumaflow_lite.reducer", from_py_object)]
#[derive(Clone)]
pub struct Metadata {
    #[pyo3(get)]
    pub interval_window: IntervalWindow,
}

#[pymethods]
impl Metadata {
    #[new]
    #[pyo3(signature = (*, interval_window: "IntervalWindow") -> "Metadata")]
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

async fn reduce_client(sock_file: String) -> PyResult<ReduceClient<tonic::transport::Channel>> {
    let endpoint = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?;

    let channel = endpoint
        .connect_with_connector(service_fn(move |_: Uri| {
            let sock = PathBuf::from(sock_file.clone());
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(sock).await?,
                ))
            }
        }))
        .await
        .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()))?;

    Ok(ReduceClient::new(channel))
}

// Poll the IsReady probe until it succeeds. There is no deadline: Python sets
// one with asyncio.timeout(), and cancellation drops this future.
async fn wait_for_ready(sock_file: String) {
    loop {
        if let Ok(mut client) = reduce_client(sock_file.clone()).await
            && let Ok(response) = client.is_ready(()).await
            && response.into_inner().ready
        {
            return;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Async Reduce Server that can be started from Python code, taking a reducer factory.
#[pyclass(name = "_ReduceAsyncServer", module = "pynumaflow_lite.reducer")]
pub struct ReduceAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl ReduceAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_ReduceAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| reduce::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| reduce::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given zero-argument factory. The server calls the
    /// factory once for each window and key set to get the async handler.
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
            crate::reduce::server::start(factory, sock_file, server_info_file, rx),
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the reduce UDS.
    #[pyo3(signature = () -> "None")]
    pub fn wait_ready<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            wait_for_ready(sock_file).await;
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
    m.add_class::<Datum>()?;
    m.add_class::<IntervalWindow>()?;
    m.add_class::<Metadata>()?;
    m.add_class::<ReduceAsyncServer>()?;
    Ok(())
}

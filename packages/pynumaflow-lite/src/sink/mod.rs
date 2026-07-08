use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use numaflow::proto::sink::sink_client::SinkClient;
use numaflow::sink;

use chrono::{DateTime, Utc};

/// Sink interface managed by Python. Python code will start the server
/// and can pass in the Python coroutine.
pub mod server;

use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::transport::Uri;
use tower::service_fn;

use pyo3::prelude::*;
use std::sync::Mutex;

fn bytes_literal(value: &[u8]) -> String {
    format!("b\"{}\"", String::from_utf8_lossy(value).escape_debug())
}

fn metadata_literal(metadata: &HashMap<String, HashMap<String, Vec<u8>>>) -> String {
    let groups: Vec<String> = metadata
        .iter()
        .map(|(group, kv)| {
            let entries: Vec<String> = kv
                .iter()
                .map(|(key, value)| format!("{:?}: {}", key, bytes_literal(value)))
                .collect();
            format!("{:?}: {{{}}}", group, entries.join(", "))
        })
        .collect();
    format!("{{{}}}", groups.join(", "))
}

fn system_metadata_to_hash_map(
    value: sink::SystemMetadata,
) -> HashMap<String, HashMap<String, Vec<u8>>> {
    let mut data = HashMap::new();
    for group in value.groups() {
        let mut kv = HashMap::new();
        for key in value.keys(&group) {
            kv.insert(key.clone(), value.value(&group, &key));
        }
        data.insert(group, kv);
    }
    data
}

fn user_metadata_to_hash_map(
    value: sink::UserMetadata,
) -> HashMap<String, HashMap<String, Vec<u8>>> {
    let mut data = HashMap::new();
    for group in value.groups() {
        let mut kv = HashMap::new();
        for key in value.keys(&group) {
            kv.insert(key.clone(), value.value(&group, &key));
        }
        data.insert(group, kv);
    }
    data
}

/// Message for OnSuccess sink response.
/// Contains information that needs to be sent to the OnSuccess sink.
#[pyclass(module = "pynumaflow_lite.sinker", from_py_object, eq)]
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Message {
    #[pyo3(get)]
    pub keys: Option<Vec<String>>,
    #[pyo3(get)]
    pub value: Vec<u8>,
    #[pyo3(get)]
    pub user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
}

#[pymethods]
impl Message {
    /// Create a new Message with the given value.
    /// Keys and user_metadata are optional.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, user_metadata: "dict[str, dict[str, bytes]] | None"=None) -> "Message")]
    fn new(
        value: Vec<u8>,
        keys: Option<Vec<String>>,
        user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    ) -> Self {
        Self {
            value,
            keys,
            user_metadata,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(value={}, keys={}, user_metadata={})",
            bytes_literal(&self.value),
            self.keys
                .as_ref()
                .map_or_else(|| "None".to_string(), |keys| format!("{keys:?}")),
            self.user_metadata
                .as_ref()
                .map_or_else(|| "None".to_string(), metadata_literal)
        )
    }
}

impl From<Message> for sink::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            user_metadata: value.user_metadata.map(|m| {
                m.into_iter()
                    .map(|(key, key_value)| (key, sink::KeyValueGroup { key_value }))
                    .collect()
            }),
        }
    }
}

/// Response for a single datum in the sink.
#[pyclass(module = "pynumaflow_lite.sinker", from_py_object, eq)]
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    #[pyo3(get)]
    pub id: String,
    pub response_type: ResponseType,
    #[pyo3(get)]
    pub error: Option<String>,
    pub serve_response: Option<Vec<u8>>,
    pub on_success_msg: Option<Message>,
}

#[pymethods]
impl Response {
    /// Create a success response.
    #[staticmethod]
    #[pyo3(signature = (id: "str") -> "Response")]
    fn success(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Success,
            error: None,
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a failure response with an error message.
    #[staticmethod]
    #[pyo3(signature = (id: "str", error: "str") -> "Response")]
    fn failure(id: String, error: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Failure,
            error: Some(error),
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a fallback response to forward to fallback sink.
    #[staticmethod]
    #[pyo3(signature = (id: "str") -> "Response")]
    fn fallback(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Fallback,
            error: None,
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a serve response with payload for serving store.
    #[staticmethod]
    #[pyo3(signature = (id: "str", payload: "bytes") -> "Response")]
    fn serve(id: String, payload: Vec<u8>) -> Self {
        Self {
            id,
            response_type: ResponseType::Serve,
            error: None,
            serve_response: Some(payload),
            on_success_msg: None,
        }
    }

    /// Create an OnSuccess response with optional message.
    /// If message is None, the original message will be sent to onSuccess sink.
    #[staticmethod]
    #[pyo3(signature = (id: "str", message: "Message | None"=None) -> "Response")]
    fn on_success(id: String, message: Option<Message>) -> Self {
        Self {
            id,
            response_type: ResponseType::OnSuccess,
            error: None,
            serve_response: None,
            on_success_msg: message,
        }
    }

    fn __repr__(&self) -> String {
        match self.response_type {
            ResponseType::Success => format!("Response.success(id={:?})", self.id),
            ResponseType::Failure => format!(
                "Response.failure(id={:?}, error={:?})",
                self.id,
                self.error.as_deref().unwrap_or_default()
            ),
            ResponseType::Fallback => format!("Response.fallback(id={:?})", self.id),
            ResponseType::Serve => format!(
                "Response.serve(id={:?}, payload={})",
                self.id,
                bytes_literal(self.serve_response.as_deref().unwrap_or_default())
            ),
            ResponseType::OnSuccess => format!(
                "Response.on_success(id={:?}, message={})",
                self.id,
                self.on_success_msg
                    .as_ref()
                    .map_or_else(|| "None".to_string(), |m| m.__repr__())
            ),
        }
    }
}

/// Internal enum to track response type
#[derive(Clone, Debug, PartialEq)]
pub enum ResponseType {
    Success,
    Failure,
    Fallback,
    Serve,
    OnSuccess,
}

impl From<Response> for sink::Response {
    fn from(value: Response) -> Self {
        let response_type = match value.response_type {
            ResponseType::Success => sink::ResponseType::Success,
            ResponseType::Failure => sink::ResponseType::Failure,
            ResponseType::Fallback => sink::ResponseType::FallBack,
            ResponseType::Serve => sink::ResponseType::Serve,
            ResponseType::OnSuccess => sink::ResponseType::OnSuccess,
        };

        Self {
            id: value.id,
            response_type,
            err: value.error,
            serve_response: value.serve_response,
            on_success_msg: value.on_success_msg.map(|m| m.into()),
        }
    }
}

/// The incoming Datum for Sink
#[pyclass(module = "pynumaflow_lite.sinker")]
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
    /// ID is the unique id of the message to be sent to the Sink.
    #[pyo3(get)]
    pub id: String,
    /// Headers for the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
    /// User metadata for the message.
    #[pyo3(get)]
    pub user_metadata: HashMap<String, HashMap<String, Vec<u8>>>,
    /// System metadata for the message.
    #[pyo3(get)]
    pub system_metadata: HashMap<String, HashMap<String, Vec<u8>>>,
}

#[pymethods]
impl Datum {
    #[new]
    #[pyo3(signature = (
        *,
        keys: "list[str] | None"=None,
        value: "bytes | None"=None,
        id: "str | None"=None,
        event_time: "datetime.datetime | None"=None,
        watermark: "datetime.datetime | None"=None,
        headers: "dict[str, str] | None"=None,
        user_metadata: "dict[str, dict[str, bytes]] | None"=None,
        system_metadata: "dict[str, dict[str, bytes]] | None"=None,
    ) -> "Datum")]
    #[allow(clippy::too_many_arguments)]
    fn new(
        keys: Option<Vec<String>>,
        value: Option<Vec<u8>>,
        id: Option<String>,
        event_time: Option<DateTime<Utc>>,
        watermark: Option<DateTime<Utc>>,
        headers: Option<HashMap<String, String>>,
        user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
        system_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    ) -> Self {
        Self {
            keys: keys.unwrap_or_default(),
            value: value.unwrap_or_default(),
            watermark: watermark.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            event_time: event_time.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            id: id.unwrap_or_default(),
            headers: headers.unwrap_or_default(),
            user_metadata: user_metadata.unwrap_or_default(),
            system_metadata: system_metadata.unwrap_or_default(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={}, watermark={}, event_time={}, id={:?}, headers={:?}, user_metadata={}, system_metadata={})",
            self.keys,
            bytes_literal(&self.value),
            self.watermark,
            self.event_time,
            self.id,
            self.headers,
            metadata_literal(&self.user_metadata),
            metadata_literal(&self.system_metadata)
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<sink::SinkRequest> for Datum {
    fn from(value: sink::SinkRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: value.watermark,
            event_time: value.event_time,
            id: value.id,
            headers: value.headers,
            user_metadata: user_metadata_to_hash_map(value.user_metadata),
            system_metadata: system_metadata_to_hash_map(value.system_metadata),
        }
    }
}

/// Python-visible async iterator that yields Datum items from a Tokio mpsc channel.
/// This is a thin wrapper around the generic AsyncChannelStream implementation.
#[pyclass(module = "pynumaflow_lite.sinker")]
pub struct PyAsyncDatumStream {
    inner: crate::pyiterables::AsyncChannelStream<Datum>,
}

#[pymethods]
impl PyAsyncDatumStream {
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

async fn sink_client(sock_file: String) -> PyResult<SinkClient<tonic::transport::Channel>> {
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

    Ok(SinkClient::new(channel))
}

async fn wait_for_ready(sock_file: String, timeout: Duration) -> PyResult<()> {
    let deadline = Instant::now() + timeout;

    loop {
        if let Ok(mut client) = sink_client(sock_file.clone()).await
            && let Ok(response) = client.is_ready(()).await
            && response.into_inner().ready
        {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(pyo3::PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                "timed out waiting for sink server readiness",
            ));
        }

        tokio::time::sleep(std::cmp::min(
            Duration::from_millis(100),
            deadline.saturating_duration_since(now),
        ))
        .await;
    }
}

/// Async Sink Server that can be started from Python code
#[pyclass(name = "_SinkAsyncServer", module = "pynumaflow_lite.sinker")]
pub struct SinkAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SinkAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_SinkAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| sink::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| sink::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python function.
    #[pyo3(signature = (handler: "callable") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, handler: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let server_info_file = self.server_info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::sink::server::start(handler, sock_file, server_info_file, rx).await?;
            Ok(())
        })
    }

    /// Wait until the Numaflow IsReady probe succeeds over the sink UDS.
    #[pyo3(signature = (timeout: "float"=30.0) -> "None")]
    pub fn wait_ready<'a>(&self, py: Python<'a>, timeout: f64) -> PyResult<Bound<'a, PyAny>> {
        if !timeout.is_finite() || timeout < 0.0 {
            return Err(pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "timeout must be a non-negative finite float",
            ));
        }

        let sock_file = self.sock_file.clone();
        let timeout = Duration::from_secs_f64(timeout);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            wait_for_ready(sock_file, timeout).await?;
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

/// Helper to populate a PyModule with sink types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Response>()?;
    m.add_class::<Datum>()?;
    m.add_class::<SinkAsyncServer>()?;

    Ok(())
}

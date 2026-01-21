use std::collections::HashMap;

use numaflow::sink;

use chrono::{DateTime, Utc};

/// Sink interface managed by Python. Python code will start the server
/// and can pass in the Python coroutine.
pub mod server;

use tokio::sync::mpsc;

use pyo3::prelude::*;
use std::sync::Mutex;

/// SystemMetadata wraps system-generated metadata groups per message.
/// Since sink is the last vertex in the pipeline, only GET methods are available.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Default, Debug)]
pub struct SystemMetadata {
    data: HashMap<String, HashMap<String, Vec<u8>>>,
}

#[pymethods]
impl SystemMetadata {
    #[new]
    #[pyo3(signature = () -> "SystemMetadata")]
    fn new() -> Self {
        Self::default()
    }

    /// Returns the groups of the system metadata.
    /// If there are no groups, it returns an empty list.
    #[pyo3(signature = () -> "list[str]")]
    fn groups(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Returns the keys of the system metadata for the given group.
    /// If there are no keys or the group is not present, it returns an empty list.
    #[pyo3(signature = (group: "str") -> "list[str]")]
    fn keys(&self, group: &str) -> Vec<String> {
        self.data
            .get(group)
            .map(|kv| kv.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the value of the system metadata for the given group and key.
    /// If there is no value or the group or key is not present, it returns an empty bytes.
    #[pyo3(signature = (group: "str", key: "str") -> "bytes")]
    fn value(&self, group: &str, key: &str) -> Vec<u8> {
        self.data
            .get(group)
            .and_then(|kv| kv.get(key))
            .cloned()
            .unwrap_or_default()
    }

    fn __repr__(&self) -> String {
        format!("SystemMetadata(groups={:?})", self.groups())
    }
}

impl From<sink::SystemMetadata> for SystemMetadata {
    fn from(value: sink::SystemMetadata) -> Self {
        let mut data = HashMap::new();
        for group in value.groups() {
            let mut kv = HashMap::new();
            for key in value.keys(&group) {
                kv.insert(key.clone(), value.value(&group, &key));
            }
            data.insert(group, kv);
        }
        Self { data }
    }
}

/// UserMetadata wraps user-defined metadata groups per message.
/// Since sink is the last vertex in the pipeline, only GET methods are available.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Default, Debug)]
pub struct UserMetadata {
    data: HashMap<String, HashMap<String, Vec<u8>>>,
}

#[pymethods]
impl UserMetadata {
    #[new]
    #[pyo3(signature = () -> "UserMetadata")]
    fn new() -> Self {
        Self::default()
    }

    /// Returns the groups of the user metadata.
    /// If there are no groups, it returns an empty list.
    #[pyo3(signature = () -> "list[str]")]
    fn groups(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Returns the keys of the user metadata for the given group.
    /// If there are no keys or the group is not present, it returns an empty list.
    #[pyo3(signature = (group: "str") -> "list[str]")]
    fn keys(&self, group: &str) -> Vec<String> {
        self.data
            .get(group)
            .map(|kv| kv.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the value of the user metadata for the given group and key.
    /// If there is no value or the group or key is not present, it returns an empty bytes.
    #[pyo3(signature = (group: "str", key: "str") -> "bytes")]
    fn value(&self, group: &str, key: &str) -> Vec<u8> {
        self.data
            .get(group)
            .and_then(|kv| kv.get(key))
            .cloned()
            .unwrap_or_default()
    }

    fn __repr__(&self) -> String {
        format!("UserMetadata(groups={:?})", self.groups())
    }
}

impl From<sink::UserMetadata> for UserMetadata {
    fn from(value: sink::UserMetadata) -> Self {
        let mut data = HashMap::new();
        for group in value.groups() {
            let mut kv = HashMap::new();
            for key in value.keys(&group) {
                kv.insert(key.clone(), value.value(&group, &key));
            }
            data.insert(group, kv);
        }
        Self { data }
    }
}

/// KeyValueGroup represents a group of key-value pairs for user metadata.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Default, Debug)]
pub struct KeyValueGroup {
    pub key_value: HashMap<String, Vec<u8>>,
}

#[pymethods]
impl KeyValueGroup {
    #[new]
    #[pyo3(signature = (key_value: "dict[str, bytes] | None"=None) -> "KeyValueGroup")]
    fn new(key_value: Option<HashMap<String, Vec<u8>>>) -> Self {
        Self {
            key_value: key_value.unwrap_or_default(),
        }
    }

    /// Create a KeyValueGroup from a dictionary of string to bytes.
    #[staticmethod]
    #[pyo3(signature = (key_value: "dict[str, bytes]") -> "KeyValueGroup")]
    fn from_dict(key_value: HashMap<String, Vec<u8>>) -> Self {
        Self { key_value }
    }
}

impl From<KeyValueGroup> for sink::KeyValueGroup {
    fn from(value: KeyValueGroup) -> Self {
        Self {
            key_value: value.key_value,
        }
    }
}

/// Message for OnSuccess sink response.
/// Contains information that needs to be sent to the OnSuccess sink.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Default, Debug)]
pub struct Message {
    pub keys: Option<Vec<String>>,
    pub value: Vec<u8>,
    pub user_metadata: Option<HashMap<String, KeyValueGroup>>,
}

#[pymethods]
impl Message {
    /// Create a new Message with the given value.
    /// Keys and user_metadata are optional.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, user_metadata: "dict[str, KeyValueGroup] | None"=None) -> "Message")]
    fn new(
        value: Vec<u8>,
        keys: Option<Vec<String>>,
        user_metadata: Option<HashMap<String, KeyValueGroup>>,
    ) -> Self {
        Self {
            value,
            keys,
            user_metadata,
        }
    }
}

impl From<Message> for sink::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            user_metadata: value
                .user_metadata
                .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect()),
        }
    }
}

/// Response for a single datum in the sink.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Debug)]
pub struct Response {
    pub id: String,
    pub response_type: ResponseType,
    pub err: Option<String>,
    pub serve_response: Option<Vec<u8>>,
    pub on_success_msg: Option<Message>,
}

#[pymethods]
impl Response {
    /// Create a success response.
    #[staticmethod]
    #[pyo3(signature = (id: "str") -> "Response")]
    fn as_success(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Success,
            err: None,
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a failure response with an error message.
    #[staticmethod]
    #[pyo3(signature = (id: "str", err_msg: "str") -> "Response")]
    fn as_failure(id: String, err_msg: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Failure,
            err: Some(err_msg),
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a fallback response to forward to fallback sink.
    #[staticmethod]
    #[pyo3(signature = (id: "str") -> "Response")]
    fn as_fallback(id: String) -> Self {
        Self {
            id,
            response_type: ResponseType::Fallback,
            err: None,
            serve_response: None,
            on_success_msg: None,
        }
    }

    /// Create a serve response with payload for serving store.
    #[staticmethod]
    #[pyo3(signature = (id: "str", payload: "bytes") -> "Response")]
    fn as_serve(id: String, payload: Vec<u8>) -> Self {
        Self {
            id,
            response_type: ResponseType::Serve,
            err: None,
            serve_response: Some(payload),
            on_success_msg: None,
        }
    }

    /// Create an OnSuccess response with optional message.
    /// If message is None, the original message will be sent to onSuccess sink.
    #[staticmethod]
    #[pyo3(signature = (id: "str", message: "Message | None"=None) -> "Response")]
    fn as_on_success(id: String, message: Option<Message>) -> Self {
        Self {
            id,
            response_type: ResponseType::OnSuccess,
            err: None,
            serve_response: None,
            on_success_msg: message,
        }
    }
}

/// Internal enum to track response type
#[derive(Clone, Debug)]
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
            err: value.err,
            serve_response: value.serve_response,
            on_success_msg: value.on_success_msg.map(|m| m.into()),
        }
    }
}

/// A collection of Response objects.
#[pyclass(module = "pynumaflow_lite.sinker")]
#[derive(Clone, Debug)]
pub struct Responses {
    pub(crate) responses: Vec<Response>,
}

#[pymethods]
impl Responses {
    #[new]
    #[pyo3(signature = () -> "Responses")]
    fn new() -> Self {
        Self { responses: vec![] }
    }

    /// Append a Response to the collection.
    #[pyo3(signature = (response: "Response"))]
    fn append(&mut self, response: Response) {
        self.responses.push(response);
    }

    fn __repr__(&self) -> String {
        format!("Responses(count={})", self.responses.len())
    }

    fn __str__(&self) -> String {
        format!("Responses(count={})", self.responses.len())
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
    pub eventtime: DateTime<Utc>,
    /// ID is the unique id of the message to be sent to the Sink.
    #[pyo3(get)]
    pub id: String,
    /// Headers for the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
    /// User metadata for the message.
    #[pyo3(get)]
    pub user_metadata: UserMetadata,
    /// System metadata for the message.
    #[pyo3(get)]
    pub system_metadata: SystemMetadata,
}

impl Datum {
    fn new(
        keys: Vec<String>,
        value: Vec<u8>,
        watermark: DateTime<Utc>,
        eventtime: DateTime<Utc>,
        id: String,
        headers: HashMap<String, String>,
        user_metadata: UserMetadata,
        system_metadata: SystemMetadata,
    ) -> Self {
        Self {
            keys,
            value,
            watermark,
            eventtime,
            id,
            headers,
            user_metadata,
            system_metadata,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, id={}, headers={:?}, user_metadata={:?}, system_metadata={:?})",
            self.keys,
            self.value,
            self.watermark,
            self.eventtime,
            self.id,
            self.headers,
            self.user_metadata,
            self.system_metadata
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, eventtime={}, id={}, headers={:?}, user_metadata={:?}, system_metadata={:?})",
            self.keys,
            String::from_utf8_lossy(&self.value),
            self.watermark,
            self.eventtime,
            self.id,
            self.headers,
            self.user_metadata,
            self.system_metadata
        )
    }
}

impl From<sink::SinkRequest> for Datum {
    fn from(value: sink::SinkRequest) -> Self {
        Datum::new(
            value.keys,
            value.value,
            value.watermark,
            value.event_time,
            value.id,
            value.headers,
            value.user_metadata.into(),
            value.system_metadata.into(),
        )
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

/// Async Sink Server that can be started from Python code
#[pyclass(module = "pynumaflow_lite.sinker")]
pub struct SinkAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SinkAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=sink::SOCK_ADDR.to_string(), info_file: "str | None"=sink::SERVER_INFO_FILE.to_string()) -> "SinkAsyncServer"
    )]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python function.
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
            crate::sink::server::start(py_func, sock_file, info_file, rx)
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

/// Helper to populate a PyModule with sink types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<SystemMetadata>()?;
    m.add_class::<UserMetadata>()?;
    m.add_class::<KeyValueGroup>()?;
    m.add_class::<Message>()?;
    m.add_class::<Response>()?;
    m.add_class::<Responses>()?;
    m.add_class::<Datum>()?;
    m.add_class::<PyAsyncDatumStream>()?;
    m.add_class::<SinkAsyncServer>()?;

    Ok(())
}

use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// Source interface managed by Python. It means Python code will start the server
/// and can pass in the Python function.
pub mod server;

use pyo3::prelude::*;
use std::sync::Mutex;

/// UserMetadata wraps user-defined metadata groups per message.
/// Source is the origin or the first vertex in the pipeline.
/// Here, for the first time, the user metadata can be set by the user.
#[pyclass(module = "pynumaflow_lite.sourcer")]
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

    /// Creates a new group in the user metadata.
    /// If the group already exists, this is a no-op.
    #[pyo3(signature = (group: "str"))]
    fn create_group(&mut self, group: String) {
        self.data.entry(group).or_default();
    }

    /// Adds a key-value pair to the user metadata.
    /// If the group is not present, it creates a new group.
    #[pyo3(signature = (group: "str", key: "str", value: "bytes"))]
    fn add_kv(&mut self, group: String, key: String, value: Vec<u8>) {
        self.data.entry(group).or_default().insert(key, value);
    }

    /// Removes a key from a group in the user metadata.
    /// If the key or group is not present, it's a no-op.
    #[pyo3(signature = (group: "str", key: "str"))]
    fn remove_key(&mut self, group: &str, key: &str) {
        if let Some(kv) = self.data.get_mut(group) {
            kv.remove(key);
        }
    }

    /// Removes a group from the user metadata.
    /// If the group is not present, it's a no-op.
    #[pyo3(signature = (group: "str"))]
    fn remove_group(&mut self, group: &str) {
        self.data.remove(group);
    }

    fn __repr__(&self) -> String {
        format!("UserMetadata(groups={:?})", self.groups())
    }
}

impl From<UserMetadata> for numaflow::source::UserMetadata {
    fn from(value: UserMetadata) -> Self {
        let mut user_metadata = numaflow::source::UserMetadata::new();
        for (group, kv_map) in value.data {
            for (key, val) in kv_map {
                user_metadata.add_kv(group.clone(), key, val);
            }
        }
        user_metadata
    }
}

/// A message to be sent from the source.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct Message {
    /// The payload of the message.
    #[pyo3(get)]
    pub payload: Vec<u8>,
    /// The offset of the message (not directly exposed, use offset property).
    pub(crate) offset: PyOffset,
    /// The event time of the message.
    #[pyo3(get)]
    pub event_time: DateTime<Utc>,
    /// Keys of the message.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// Headers of the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
    /// User metadata for the message.
    pub user_metadata: Option<UserMetadata>,
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given payload, offset, event_time, keys, headers, and user_metadata.
    #[new]
    #[pyo3(signature = (payload: "bytes", offset: "Offset", event_time: "datetime", keys: "list[str] | None"=None, headers: "dict[str, str] | None"=None, user_metadata: "UserMetadata | None"=None) -> "Message"
    )]
    fn new(
        payload: Vec<u8>,
        offset: PyOffset,
        event_time: DateTime<Utc>,
        keys: Option<Vec<String>>,
        headers: Option<HashMap<String, String>>,
        user_metadata: Option<UserMetadata>,
    ) -> Self {
        Self {
            payload,
            offset,
            event_time,
            keys: keys.unwrap_or_default(),
            headers: headers.unwrap_or_default(),
            user_metadata,
        }
    }

    /// Get the offset of the message.
    #[getter]
    fn offset(&self) -> PyOffset {
        self.offset.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(payload={:?}, offset={:?}, event_time={}, keys={:?}, headers={:?}, user_metadata={:?})",
            self.payload, self.offset, self.event_time, self.keys, self.headers, self.user_metadata
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Message(payload={:?}, offset={:?}, event_time={}, keys={:?}, headers={:?}, user_metadata={:?})",
            String::from_utf8_lossy(&self.payload),
            self.offset,
            self.event_time,
            self.keys,
            self.headers,
            self.user_metadata
        )
    }
}

impl From<Message> for numaflow::source::Message {
    fn from(value: Message) -> Self {
        Self {
            value: value.payload,
            offset: value.offset.into(),
            event_time: value.event_time,
            keys: value.keys,
            headers: value.headers,
            user_metadata: value.user_metadata.map(|m| m.into()),
        }
    }
}

/// The offset of a message.
#[pyclass(module = "pynumaflow_lite.sourcer", name = "Offset")] // this to avoid conflict with the Offset in the source module
#[derive(Clone, Debug)]
pub struct PyOffset {
    /// Offset value in bytes.
    #[pyo3(get)]
    pub offset: Vec<u8>,
    /// Partition ID of the message.
    #[pyo3(get)]
    pub partition_id: i32,
}

#[pymethods]
impl PyOffset {
    /// Create a new [Offset] with the given offset bytes and partition_id.
    #[new]
    #[pyo3(signature = (offset: "bytes", partition_id: "int"=0) -> "Offset")]
    fn new(offset: Vec<u8>, partition_id: i32) -> Self {
        Self {
            offset,
            partition_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Offset(offset={:?}, partition_id={})",
            self.offset, self.partition_id
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Offset(offset={:?}, partition_id={})",
            String::from_utf8_lossy(&self.offset),
            self.partition_id
        )
    }
}

impl From<PyOffset> for numaflow::source::Offset {
    fn from(value: PyOffset) -> Self {
        Self {
            offset: value.offset,
            partition_id: value.partition_id,
        }
    }
}

impl From<numaflow::source::Offset> for PyOffset {
    fn from(value: numaflow::source::Offset) -> Self {
        Self {
            offset: value.offset,
            partition_id: value.partition_id,
        }
    }
}

/// A request to read messages from the source.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct ReadRequest {
    /// The number of messages to read.
    #[pyo3(get)]
    pub num_records: u64,
    /// Request timeout in milliseconds.
    #[pyo3(get)]
    pub timeout_ms: u64,
}

#[pymethods]
impl ReadRequest {
    /// Create a new [ReadRequest] with the given num_records and timeout_ms.
    #[new]
    #[pyo3(signature = (num_records: "int", timeout_ms: "int"=1000) -> "ReadRequest")]
    fn new(num_records: u64, timeout_ms: u64) -> Self {
        Self {
            num_records,
            timeout_ms,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ReadRequest(num_records={}, timeout_ms={})",
            self.num_records, self.timeout_ms
        )
    }
}

impl From<numaflow::source::SourceReadRequest> for ReadRequest {
    fn from(value: numaflow::source::SourceReadRequest) -> Self {
        Self {
            num_records: value.count as u64,
            timeout_ms: value.timeout.as_millis() as u64,
        }
    }
}

/// A request to acknowledge messages.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct AckRequest {
    /// The offsets to acknowledge.
    #[pyo3(get)]
    pub offsets: Vec<PyOffset>,
}

#[pymethods]
impl AckRequest {
    /// Create a new [AckRequest] with the given offsets.
    #[new]
    #[pyo3(signature = (offsets: "list[Offset]") -> "AckRequest")]
    fn new(offsets: Vec<PyOffset>) -> Self {
        Self { offsets }
    }

    fn __repr__(&self) -> String {
        format!("AckRequest(offsets={:?})", self.offsets)
    }
}

/// A request to negatively acknowledge messages.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct NackRequest {
    /// The offsets to negatively acknowledge.
    #[pyo3(get)]
    pub offsets: Vec<PyOffset>,
}

#[pymethods]
impl NackRequest {
    /// Create a new [NackRequest] with the given offsets.
    #[new]
    #[pyo3(signature = (offsets: "list[Offset]") -> "NackRequest")]
    fn new(offsets: Vec<PyOffset>) -> Self {
        Self { offsets }
    }

    fn __repr__(&self) -> String {
        format!("NackRequest(offsets={:?})", self.offsets)
    }
}

/// Response for pending messages count.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct PendingResponse {
    /// The number of pending messages. -1 if the source doesn't support detecting backlog.
    #[pyo3(get)]
    pub count: i64,
}

#[pymethods]
impl PendingResponse {
    /// Create a new [PendingResponse] with the given count.
    #[new]
    #[pyo3(signature = (count: "int"=0) -> "PendingResponse")]
    fn new(count: i64) -> Self {
        Self { count }
    }

    fn __repr__(&self) -> String {
        format!("PendingResponse(count={})", self.count)
    }
}

/// Response for partitions.
#[pyclass(module = "pynumaflow_lite.sourcer")]
#[derive(Clone, Debug)]
pub struct PartitionsResponse {
    /// The list of partition IDs.
    #[pyo3(get)]
    pub partitions: Vec<i32>,
}

#[pymethods]
impl PartitionsResponse {
    /// Create a new [PartitionsResponse] with the given partitions.
    #[new]
    #[pyo3(signature = (partitions: "list[int]") -> "PartitionsResponse")]
    fn new(partitions: Vec<i32>) -> Self {
        Self { partitions }
    }

    fn __repr__(&self) -> String {
        format!("PartitionsResponse(partitions={:?})", self.partitions)
    }
}

/// Async Source Server that can be started from Python code which will run the Python UDF function.
#[pyclass(module = "pynumaflow_lite.sourcer")]
pub struct SourceAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SourceAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=numaflow::source::SOCK_ADDR.to_string(), info_file: "str | None"=numaflow::source::SERVER_INFO_FILE.to_string()) -> "SourceAsyncServer"
    )]
    fn new(sock_file: String, info_file: String) -> Self {
        Self {
            sock_file,
            info_file,
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python source handler.
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
            crate::source::server::start(py_func, sock_file, info_file, rx)
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

/// Helper to populate a PyModule with source types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<UserMetadata>()?;
    m.add_class::<Message>()?;
    m.add_class::<PyOffset>()?;
    m.add_class::<ReadRequest>()?;
    m.add_class::<AckRequest>()?;
    m.add_class::<NackRequest>()?;
    m.add_class::<PendingResponse>()?;
    m.add_class::<PartitionsResponse>()?;
    m.add_class::<SourceAsyncServer>()?;

    Ok(())
}

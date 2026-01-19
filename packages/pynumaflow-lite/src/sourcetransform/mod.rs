use std::collections::HashMap;

use numaflow::sourcetransform;

use chrono::{DateTime, Utc};

/// SourceTransform interface managed by Python. It means Python code will start the server
/// and can pass in the Python function.
pub mod server;

use pyo3::prelude::*;
use std::sync::Mutex;

/// SystemMetadata wraps system-generated metadata groups per message.
/// It is read-only to UDFs.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
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

impl From<sourcetransform::SystemMetadata> for SystemMetadata {
    fn from(value: sourcetransform::SystemMetadata) -> Self {
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
/// Users can read and write to this metadata.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
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

impl From<sourcetransform::UserMetadata> for UserMetadata {
    fn from(value: sourcetransform::UserMetadata) -> Self {
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

impl From<UserMetadata> for sourcetransform::UserMetadata {
    fn from(value: UserMetadata) -> Self {
        let mut user_metadata = sourcetransform::UserMetadata::new();
        for (group, kv_map) in value.data {
            for (key, val) in kv_map {
                user_metadata.add_kv(group.clone(), key, val);
            }
        }
        user_metadata
    }
}

/// A collection of [Message]s.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
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

    /// Append a [Message] to the collection.
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

/// A message to be sent to the next vertex with event time transformation.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
#[derive(Clone, Default, Debug)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Time for the given event. This will be used for tracking watermarks.
    pub event_time: DateTime<Utc>,
    /// Tags are used for conditional forwarding.
    pub tags: Option<Vec<String>>,
    /// User metadata for the message.
    pub user_metadata: Option<UserMetadata>,
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given value, event_time, keys, tags, and user_metadata.
    #[new]
    #[pyo3(signature = (value: "bytes", event_time: "datetime.datetime", keys: "list[str] | None"=None, tags: "list[str] | None"=None, user_metadata: "UserMetadata | None"=None) -> "Message"
    )]
    fn new(
        value: Vec<u8>,
        event_time: DateTime<Utc>,
        keys: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        user_metadata: Option<UserMetadata>,
    ) -> Self {
        Self {
            keys,
            value,
            event_time,
            tags,
            user_metadata,
        }
    }

    /// Drop a [Message], do not forward to the next vertex.
    /// Event time is required because even though a message is dropped,
    /// it is still considered as being processed, hence the watermark should be updated.
    #[pyo3(signature = (event_time: "datetime.datetime"))]
    #[staticmethod]
    fn message_to_drop(event_time: DateTime<Utc>) -> Self {
        Self {
            keys: None,
            value: vec![],
            event_time,
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
            user_metadata: None,
        }
    }
}

impl From<Message> for sourcetransform::Message {
    fn from(value: Message) -> Self {
        let mut msg = Self::new(value.value, value.event_time)
            .with_keys(value.keys.unwrap_or_default())
            .with_tags(value.tags.unwrap_or_default());

        if let Some(user_metadata) = value.user_metadata {
            msg = msg.with_user_metadata(user_metadata.into());
        }

        msg
    }
}

/// The incoming [SourceTransformRequest] accessible in Python function.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
pub struct Datum {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// Watermark represented by time is a guarantee that we will not see an element older than this time.
    #[pyo3(get)]
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    #[pyo3(get)]
    pub event_time: DateTime<Utc>,
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
        event_time: DateTime<Utc>,
        headers: HashMap<String, String>,
        user_metadata: UserMetadata,
        system_metadata: SystemMetadata,
    ) -> Self {
        Self {
            keys,
            value,
            watermark,
            event_time,
            headers,
            user_metadata,
            system_metadata,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, event_time={}, headers={:?}, user_metadata={:?}, system_metadata={:?})",
            self.keys,
            self.value,
            self.watermark,
            self.event_time,
            self.headers,
            self.user_metadata,
            self.system_metadata
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={:?}, watermark={}, event_time={}, headers={:?}, user_metadata={:?}, system_metadata={:?})",
            self.keys,
            String::from_utf8_lossy(&self.value),
            self.watermark,
            self.event_time,
            self.headers,
            self.user_metadata,
            self.system_metadata
        )
    }
}

impl From<sourcetransform::SourceTransformRequest> for Datum {
    fn from(value: sourcetransform::SourceTransformRequest) -> Self {
        Datum::new(
            value.keys,
            value.value,
            value.watermark,
            value.eventtime,
            value.headers,
            value.user_metadata.into(),
            value.system_metadata.into(),
        )
    }
}

/// Async SourceTransform Server that can be started from Python code which will run the Python UDF function.
#[pyclass(module = "pynumaflow_lite.sourcetransformer")]
pub struct SourceTransformAsyncServer {
    sock_file: String,
    info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SourceTransformAsyncServer {
    #[new]
    #[pyo3(signature = (sock_file: "str | None"=sourcetransform::SOCK_ADDR.to_string(), info_file: "str | None"=sourcetransform::SERVER_INFO_FILE.to_string()) -> "SourceTransformAsyncServer"
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
            crate::sourcetransform::server::start(py_func, sock_file, info_file, rx)
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

/// Helper to populate a PyModule with sourcetransform types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<SystemMetadata>()?;
    m.add_class::<UserMetadata>()?;
    m.add_class::<Messages>()?;
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<SourceTransformAsyncServer>()?;

    Ok(())
}

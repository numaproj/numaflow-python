use std::collections::HashMap;

use numaflow::batchmap;

use chrono::{DateTime, Utc};

/// BatchMap interface managed by Python. Python code will start the server
/// and can pass in the Python coroutine.
pub mod server;

use tokio::sync::mpsc;

use pyo3::prelude::*;
use std::sync::Mutex;

use crate::map::{
    hash_map_to_user_metadata, system_metadata_to_hash_map, user_metadata_to_hash_map,
};
use crate::nack::NackOptions;
use crate::pyrs::{bytes_literal, py_repr};

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
    /// User metadata for the message.
    #[pyo3(get)]
    pub user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    /// Options sent back to the source when nacking the message.
    #[pyo3(get)]
    pub nack_options: Option<NackOptions>,
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given value. Keys, tags, and user_metadata are optional.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None, user_metadata: "dict[str, dict[str, bytes]] | None"=None) -> "Message"
    )]
    fn new(
        value: Vec<u8>,
        keys: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    ) -> Self {
        Self {
            keys,
            value,
            tags,
            user_metadata,
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
            user_metadata: None,
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
            user_metadata: None,
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
            user_metadata: None,
            nack_options: None,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Message(value={}, keys={}, tags={}, user_metadata={}, nack_options={})",
            bytes_literal(&self.value),
            py_repr(py, &self.keys)?,
            py_repr(py, &self.tags)?,
            py_repr(py, &self.user_metadata)?,
            py_repr(py, self.nack_options.clone())?,
        ))
    }
}

impl From<Message> for batchmap::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
            user_metadata: value.user_metadata.map(hash_map_to_user_metadata),
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
        id: "str",
        keys: "list[str] | None"=None,
        value: "bytes | None"=None,
        event_time: "datetime.datetime | None"=None,
        watermark: "datetime.datetime | None"=None,
        headers: "dict[str, str] | None"=None,
        user_metadata: "dict[str, dict[str, bytes]] | None"=None,
        system_metadata: "dict[str, dict[str, bytes]] | None"=None,
    ) -> "Datum")]
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: String,
        keys: Option<Vec<String>>,
        value: Option<Vec<u8>>,
        event_time: Option<DateTime<Utc>>,
        watermark: Option<DateTime<Utc>>,
        headers: Option<HashMap<String, String>>,
        user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
        system_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    ) -> Self {
        Self {
            id,
            keys: keys.unwrap_or_default(),
            value: value.unwrap_or_default(),
            watermark: watermark.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            event_time: event_time.unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            headers: headers.unwrap_or_default(),
            user_metadata: user_metadata.unwrap_or_default(),
            system_metadata: system_metadata.unwrap_or_default(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Datum(id={}, keys={}, value={}, watermark={}, event_time={}, headers={}, user_metadata={}, system_metadata={})",
            py_repr(py, &self.id)?,
            py_repr(py, &self.keys)?,
            bytes_literal(&self.value),
            self.watermark,
            self.event_time,
            py_repr(py, &self.headers)?,
            py_repr(py, &self.user_metadata)?,
            py_repr(py, &self.system_metadata)?,
        ))
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

impl From<batchmap::Datum> for Datum {
    fn from(value: batchmap::Datum) -> Self {
        Self {
            id: value.id,
            keys: value.keys,
            value: value.value,
            watermark: value.watermark,
            event_time: value.event_time,
            headers: value.headers,
            user_metadata: user_metadata_to_hash_map(value.user_metadata),
            system_metadata: system_metadata_to_hash_map(value.system_metadata),
        }
    }
}

/// BatchResponse mirrors numaflow::batchmap::BatchResponse for Python
#[pyclass(module = "pynumaflow_lite.batchmapper", from_py_object)]
#[derive(Clone, Debug)]
pub struct BatchResponse {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub messages: Vec<Message>,
}

#[pymethods]
impl BatchResponse {
    /// Create a new [BatchResponse] for the given datum id, with optional messages.
    #[new]
    #[pyo3(signature = (id: "str", *messages: "Message") -> "BatchResponse")]
    fn new(id: String, messages: Vec<Message>) -> Self {
        Self { id, messages }
    }

    #[pyo3(signature = (message))]
    fn append(&mut self, message: Message) {
        self.messages.push(message);
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "BatchResponse(id={}, messages={})",
            py_repr(py, &self.id)?,
            py_repr(py, self.messages.clone())?,
        ))
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
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
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
            crate::batchmap::server::start(handler, sock_file, info_file, rx),
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the batchmap UDS.
    #[pyo3(signature = () -> "None")]
    pub fn wait_ready<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            crate::map::wait_for_ready(sock_file).await;
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
    m.add_class::<NackOptions>()?;
    m.add_class::<BatchResponse>()?;
    m.add_class::<BatchMapAsyncServer>()?;
    m.add_class::<PyAsyncDatumStream>()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{DateTime, Utc};
    use numaflow::batchmap;

    use super::Datum;

    #[test]
    fn datum_from_batchmap_datum_keeps_times_and_metadata() {
        let event_time = DateTime::<Utc>::from_timestamp(1_000, 0).unwrap();
        let watermark = DateTime::<Utc>::from_timestamp(2_000, 0).unwrap();
        let mut user_metadata = batchmap::UserMetadata::new();
        user_metadata.add_kv(
            "user_group".to_string(),
            "user_key".to_string(),
            b"user_value".to_vec(),
        );

        let datum: Datum = batchmap::Datum {
            keys: vec!["k".to_string()],
            value: b"v".to_vec(),
            watermark,
            event_time,
            id: "id-1".to_string(),
            headers: HashMap::new(),
            user_metadata,
            system_metadata: batchmap::SystemMetadata::new(),
        }
        .into();

        assert_eq!(datum.event_time, event_time);
        assert_eq!(datum.watermark, watermark);
        assert_eq!(datum.user_metadata["user_group"]["user_key"], b"user_value");
        assert!(datum.system_metadata.is_empty());
    }
}

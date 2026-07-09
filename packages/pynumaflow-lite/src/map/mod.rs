use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use numaflow::map;
use numaflow::proto::map::map_client::MapClient;

use chrono::{DateTime, Utc};

/// Map interface managed by Python. It means Python code will start the server
/// and can pass in the Python function.
pub mod server;

use tokio::net::UnixStream;
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
    value: map::SystemMetadata,
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
    value: map::UserMetadata,
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

/// A message to be sent to the next vertex.
#[pyclass(module = "pynumaflow_lite.mapper", from_py_object, eq)]
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
    /// User metadata for the message.
    #[pyo3(get)]
    pub user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
}

#[pymethods]
impl Message {
    /// Create a new Message with the given value. Keys, tags, and user_metadata are optional.
    #[new]
    #[pyo3(signature = (value: "bytes", keys: "list[str] | None"=None, tags: "list[str] | None"=None, user_metadata: "dict[str, dict[str, bytes]] | None"=None) -> "Message")]
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
        }
    }

    /// A Message marked to be dropped, i.e. not forwarded to the next vertex.
    #[staticmethod]
    #[pyo3(signature = () -> "Message")]
    fn to_drop() -> Self {
        Self {
            keys: None,
            value: vec![],
            tags: Some(vec![numaflow::shared::DROP.to_string()]),
            user_metadata: None,
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
            self.user_metadata
                .as_ref()
                .map_or_else(|| "None".to_string(), metadata_literal),
        )
    }
}

impl From<Message> for map::Message {
    fn from(value: Message) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
            user_metadata: value.user_metadata.map(|m| {
                let mut umd = map::UserMetadata::new();
                for (group, kv) in m {
                    for (key, val) in kv {
                        umd.add_kv(group.clone(), key, val);
                    }
                }
                umd
            }),
        }
    }
}

/// The incoming Datum passed to the map handler. It carries the event's keys, value,
/// event_time, watermark, headers, and the user/system metadata.
#[pyclass(module = "pynumaflow_lite.mapper")]
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
        event_time: "datetime.datetime | None"=None,
        watermark: "datetime.datetime | None"=None,
        headers: "dict[str, str] | None"=None,
        user_metadata: "dict[str, dict[str, bytes]] | None"=None,
        system_metadata: "dict[str, dict[str, bytes]] | None"=None,
    ) -> "Datum")]
    fn new(
        keys: Option<Vec<String>>,
        value: Option<Vec<u8>>,
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
            headers: headers.unwrap_or_default(),
            user_metadata: user_metadata.unwrap_or_default(),
            system_metadata: system_metadata.unwrap_or_default(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Datum(keys={:?}, value={}, watermark={}, event_time={}, headers={:?}, user_metadata={}, system_metadata={})",
            self.keys,
            bytes_literal(&self.value),
            self.watermark,
            self.event_time,
            self.headers,
            metadata_literal(&self.user_metadata),
            metadata_literal(&self.system_metadata)
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<map::MapRequest> for Datum {
    fn from(value: map::MapRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: value.watermark,
            event_time: value.eventtime,
            headers: value.headers,
            user_metadata: user_metadata_to_hash_map(value.user_metadata),
            system_metadata: system_metadata_to_hash_map(value.system_metadata),
        }
    }
}

async fn map_grpc_client(sock_file: String) -> PyResult<MapClient<tonic::transport::Channel>> {
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

    Ok(MapClient::new(channel))
}

async fn wait_for_ready(sock_file: String, timeout: Duration) -> PyResult<()> {
    let deadline = Instant::now() + timeout;

    loop {
        if let Ok(mut client) = map_grpc_client(sock_file.clone()).await
            && let Ok(response) = client.is_ready(()).await
            && response.into_inner().ready
        {
            return Ok(());
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(pyo3::PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                "timed out waiting for map server readiness",
            ));
        }

        tokio::time::sleep(std::cmp::min(
            Duration::from_millis(100),
            deadline.saturating_duration_since(now),
        ))
        .await;
    }
}

/// Async Map Server that can be started from Python code which will run the Python UDF function.
#[pyclass(name = "_MapAsyncServer", module = "pynumaflow_lite.mapper")]
pub struct MapAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl MapAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_MapAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| map::SOCK_ADDR.to_string()),
            server_info_file: server_info_file.unwrap_or_else(|| map::SERVER_INFO_FILE.to_string()),
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
            crate::map::server::start(handler, sock_file, server_info_file, rx).await?;
            Ok(())
        })
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

/// Helper to populate a PyModule with map types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<Datum>()?;
    m.add_class::<MapAsyncServer>()?;

    Ok(())
}

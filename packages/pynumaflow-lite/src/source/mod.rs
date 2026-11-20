use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use numaflow::proto::source::source_client::SourceClient;
use numaflow::source;
use pyo3::prelude::*;
use tokio::net::UnixStream;
use tonic::transport::Uri;
use tower::service_fn;

use crate::nack::NackOptions;
use crate::pyrs::{bytes_literal, py_repr};

/// Source interface managed by Python. It means Python code will start the server
/// and can pass in the Python Sourcer.
pub mod server;

/// The offset of a message. Offsets are immutable and hashable, so they can be
/// kept in sets and used as dict keys to track in-flight messages.
#[pyclass(
    module = "pynumaflow_lite.sourcer",
    name = "Offset",
    frozen,
    eq,
    hash,
    from_py_object
)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PyOffset {
    /// The offset value, as defined by the source.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// The partition the offset belongs to.
    #[pyo3(get)]
    pub partition_id: i32,
}

#[pymethods]
impl PyOffset {
    #[new]
    #[pyo3(signature = (value: "bytes", partition_id: "int"=0) -> "Offset")]
    fn new(value: Vec<u8>, partition_id: i32) -> Self {
        Self {
            value,
            partition_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Offset(value={}, partition_id={})",
            bytes_literal(&self.value),
            self.partition_id
        )
    }
}

impl From<PyOffset> for source::Offset {
    fn from(value: PyOffset) -> Self {
        Self {
            offset: value.value,
            partition_id: value.partition_id,
        }
    }
}

impl From<source::Offset> for PyOffset {
    fn from(value: source::Offset) -> Self {
        Self {
            value: value.offset,
            partition_id: value.partition_id,
        }
    }
}

/// A message read from the source.
#[pyclass(module = "pynumaflow_lite.sourcer", frozen, eq, from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    /// The value passed to the next vertex.
    #[pyo3(get)]
    pub value: Vec<u8>,
    /// The offset of the message. The same offset is later given to ack or nack.
    #[pyo3(get)]
    pub offset: PyOffset,
    /// The time at which the event happened.
    #[pyo3(get)]
    pub event_time: DateTime<Utc>,
    /// Keys of the message.
    #[pyo3(get)]
    pub keys: Vec<String>,
    /// Headers of the message.
    #[pyo3(get)]
    pub headers: HashMap<String, String>,
    /// User metadata, as {group: {key: value}}. The source is the first vertex
    /// of the pipeline, so it is the first place where user metadata is set.
    #[pyo3(get)]
    pub user_metadata: HashMap<String, HashMap<String, Vec<u8>>>,
}

#[pymethods]
impl Message {
    #[new]
    #[pyo3(signature = (
        value: "bytes",
        offset: "Offset",
        event_time: "datetime.datetime",
        keys: "list[str] | None"=None,
        headers: "dict[str, str] | None"=None,
        user_metadata: "dict[str, dict[str, bytes]] | None"=None,
    ) -> "Message")]
    fn new(
        value: Vec<u8>,
        offset: PyOffset,
        event_time: DateTime<Utc>,
        keys: Option<Vec<String>>,
        headers: Option<HashMap<String, String>>,
        user_metadata: Option<HashMap<String, HashMap<String, Vec<u8>>>>,
    ) -> Self {
        Self {
            value,
            offset,
            event_time,
            keys: keys.unwrap_or_default(),
            headers: headers.unwrap_or_default(),
            user_metadata: user_metadata.unwrap_or_default(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Message(value={}, offset={}, event_time={}, keys={}, headers={}, user_metadata={})",
            bytes_literal(&self.value),
            self.offset.__repr__(),
            py_repr(py, self.event_time)?,
            py_repr(py, &self.keys)?,
            py_repr(py, &self.headers)?,
            py_repr(py, &self.user_metadata)?,
        ))
    }
}

impl From<Message> for source::Message {
    fn from(value: Message) -> Self {
        let user_metadata = (!value.user_metadata.is_empty()).then(|| {
            let mut umd = source::UserMetadata::new();
            for (group, kv) in value.user_metadata {
                umd.create_group(group.clone());
                for (key, val) in kv {
                    umd.add_kv(group.clone(), key, val);
                }
            }
            umd
        });

        Self {
            value: value.value,
            offset: value.offset.into(),
            event_time: value.event_time,
            keys: value.keys,
            headers: value.headers,
            user_metadata,
        }
    }
}

/// A request to read messages from the source.
#[pyclass(module = "pynumaflow_lite.sourcer", frozen, eq, from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct ReadRequest {
    /// The maximum number of messages to read.
    #[pyo3(get)]
    pub num_records: u64,
    /// How long the read may take. Return early, with fewer messages, when it runs out.
    #[pyo3(get)]
    pub timeout: Duration,
}

#[pymethods]
impl ReadRequest {
    #[new]
    #[pyo3(signature = (
        num_records: "int",
        timeout: "datetime.timedelta"=Duration::from_secs(1),
    ) -> "ReadRequest")]
    fn new(num_records: u64, timeout: Duration) -> Self {
        Self {
            num_records,
            timeout,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ReadRequest(num_records={}, timeout={})",
            self.num_records,
            py_repr(py, self.timeout)?,
        ))
    }
}

impl From<source::SourceReadRequest> for ReadRequest {
    fn from(value: source::SourceReadRequest) -> Self {
        Self {
            num_records: value.count as u64,
            timeout: value.timeout,
        }
    }
}

/// An offset to negatively acknowledge, with the nack options for it.
#[pyclass(module = "pynumaflow_lite.sourcer", frozen, eq, from_py_object)]
#[derive(Clone, Debug, PartialEq)]
pub struct NackOffset {
    /// The offset to negatively acknowledge.
    #[pyo3(get)]
    pub offset: PyOffset,
    /// How the platform asks for the message to be redelivered, if it says so.
    #[pyo3(get)]
    pub nack_options: Option<NackOptions>,
}

#[pymethods]
impl NackOffset {
    #[new]
    #[pyo3(signature = (offset: "Offset", nack_options: "NackOptions | None"=None) -> "NackOffset")]
    fn new(offset: PyOffset, nack_options: Option<NackOptions>) -> Self {
        Self {
            offset,
            nack_options,
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "NackOffset(offset={}, nack_options={})",
            self.offset.__repr__(),
            py_repr(py, self.nack_options.clone())?,
        ))
    }
}

impl From<source::NackOffset> for NackOffset {
    fn from(value: source::NackOffset) -> Self {
        Self {
            offset: value.offset.into(),
            nack_options: value.options.map(Into::into),
        }
    }
}

async fn source_client(sock_file: String) -> PyResult<SourceClient<tonic::transport::Channel>> {
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

    Ok(SourceClient::new(channel))
}

// Poll the IsReady probe until it succeeds. There is no deadline: Python sets
// one with asyncio.timeout(), and cancellation drops this future.
async fn wait_for_ready(sock_file: String) {
    loop {
        if let Ok(mut client) = source_client(sock_file.clone()).await
            && let Ok(response) = client.is_ready(()).await
            && response.into_inner().ready
        {
            return;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Async Source Server that can be started from Python code which will run the Python Sourcer.
#[pyclass(name = "_SourceAsyncServer", module = "pynumaflow_lite.sourcer")]
pub struct SourceAsyncServer {
    sock_file: String,
    server_info_file: String,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

#[pymethods]
impl SourceAsyncServer {
    #[new]
    #[pyo3(signature = (
        sock_file: "str | None"=None,
        server_info_file: "str | None"=None,
    ) -> "_SourceAsyncServer")]
    fn new(sock_file: Option<String>, server_info_file: Option<String>) -> Self {
        Self {
            sock_file: sock_file.unwrap_or_else(|| source::SOCK_ADDR.to_string()),
            server_info_file: server_info_file
                .unwrap_or_else(|| source::SERVER_INFO_FILE.to_string()),
            shutdown_tx: Mutex::new(None),
        }
    }

    /// Start the server with the given Python Sourcer.
    #[pyo3(signature = (sourcer: "Sourcer") -> "None")]
    pub fn start<'a>(&self, py: Python<'a>, sourcer: Py<PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let sock_file = self.sock_file.clone();
        let server_info_file = self.server_info_file.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        {
            let mut guard = self.shutdown_tx.lock().unwrap();
            *guard = Some(tx);
        }

        pyo3_async_runtimes::tokio::future_into_py(
            py,
            crate::source::server::start(sourcer, sock_file, server_info_file, rx),
        )
    }

    /// Wait until the Numaflow IsReady probe succeeds over the source UDS.
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

/// Helper to populate a PyModule with source types/functions.
pub(crate) fn populate_py_module(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Message>()?;
    m.add_class::<PyOffset>()?;
    m.add_class::<ReadRequest>()?;
    m.add_class::<NackOptions>()?;
    m.add_class::<NackOffset>()?;
    m.add_class::<SourceAsyncServer>()?;

    Ok(())
}

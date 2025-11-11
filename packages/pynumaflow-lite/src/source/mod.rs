use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// Source interface managed by Python. It means Python code will start the server
/// and can pass in the Python function.
pub mod server;

use pyo3::prelude::*;
use std::sync::Mutex;

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
}

#[pymethods]
impl Message {
    /// Create a new [Message] with the given payload, offset, event_time, keys, and headers.
    #[new]
    #[pyo3(signature = (payload: "bytes", offset: "Offset", event_time: "datetime", keys: "list[str] | None"=None, headers: "dict[str, str] | None"=None) -> "Message"
    )]
    fn new(
        payload: Vec<u8>,
        offset: PyOffset,
        event_time: DateTime<Utc>,
        keys: Option<Vec<String>>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            payload,
            offset,
            event_time,
            keys: keys.unwrap_or_default(),
            headers: headers.unwrap_or_default(),
        }
    }

    /// Get the offset of the message.
    #[getter]
    fn offset(&self) -> PyOffset {
        self.offset.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Message(payload={:?}, offset={:?}, event_time={}, keys={:?}, headers={:?})",
            self.payload, self.offset, self.event_time, self.keys, self.headers
        )
    }

    fn __str__(&self) -> String {
        format!(
            "Message(payload={:?}, offset={:?}, event_time={}, keys={:?}, headers={:?})",
            String::from_utf8_lossy(&self.payload),
            self.offset,
            self.event_time,
            self.keys,
            self.headers
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

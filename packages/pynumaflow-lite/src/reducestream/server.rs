use std::sync::{Arc, Mutex};

use crate::pyiterables::PyAsyncIterStream;
use crate::pyrs::{combine_errors, format_error};
use crate::reducestream::{
    Datum as PyDatum, IntervalWindow as PyIntervalWindow, Message as PyMessage,
    Metadata as PyMetadata, PyAsyncDatumStream,
};
use numaflow::reducestream;
use numaflow::shared::ServerExtras;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use tokio_stream::StreamExt;

pub(crate) struct PyReduceStreamerCreator {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Zero-argument Python callable that returns the handler for one window
    pub(crate) factory: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

pub(crate) struct PyReduceStreamerRunner {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) factory: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl reducestream::ReduceStreamerCreator for PyReduceStreamerCreator {
    type R = PyReduceStreamerRunner;

    // The factory runs later, in reducestream(). A panic in create() would stop the
    // numaflow task executor without an error to the client, but reducestream()
    // reports a panic.
    fn create(&self) -> Self::R {
        PyReduceStreamerRunner {
            event_loop: self.event_loop.clone(),
            factory: self.factory.clone(),
            errors: self.errors.clone(),
        }
    }
}

impl PyReduceStreamerRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow calls reducestream() concurrently, so each error belongs to a
        // different window. Keep all of them. start() raises them together, which
        // lets Python format every traceback instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow catches this panic, sends a gRPC error for this window, and
        // starts the server shutdown.
        panic!("{message}");
    }
}

#[tonic::async_trait]
impl reducestream::ReduceStreamer for PyReduceStreamerRunner {
    async fn reducestream(
        &self,
        keys: Vec<String>,
        mut input: tokio::sync::mpsc::Receiver<reducestream::ReduceStreamRequest>,
        output: tokio::sync::mpsc::Sender<reducestream::Message>,
        md: &reducestream::Metadata,
    ) {
        // Create a channel to stream Datum into Python as an async iterator
        let (tx, rx) = tokio::sync::mpsc::channel::<PyDatum>(64);

        // Spawn a task forwarding incoming datums to the Python-facing channel
        let forwarder = tokio::spawn(async move {
            while let Some(req) = input.recv().await {
                if tx.send(req.into()).await.is_err() {
                    break;
                }
            }
            // When input ends, dropping tx closes the channel and that is when
            // the Python async iterable will stop.
        });

        // Call the Python handler:
        // factory()(keys: list[str], datums: AsyncIterator[Datum], md: Metadata) -> AsyncIterator[Message]
        let agen = match Python::attach(|py| -> PyResult<_> {
            let handler = self.factory.call0(py)?;

            let stream = PyAsyncDatumStream::new_with(rx);
            let interval =
                PyIntervalWindow::new(md.interval_window.start_time, md.interval_window.end_time);
            let md_py = PyMetadata::new(interval);

            handler.call1(py, (keys, stream, md_py))
        }) {
            Ok(agen) => agen,
            Err(error) => self.fail(error),
        };

        // Wrap the Python AsyncIterable in a Rust Stream that yields incrementally.
        // Items stay as Py<PyAny> so that a wrong item type gives a clear error below.
        let mut stream = match PyAsyncIterStream::<Py<PyAny>>::new(agen, self.event_loop.clone()) {
            Ok(stream) => stream,
            Err(_) => self.fail(PyErr::new::<PyTypeError, _>(
                "reducestream handler must be an async generator (return AsyncIterator[Message])",
            )),
        };

        // Forward each yielded message immediately to the output sender
        while let Some(item) = stream.next().await {
            let obj = match item {
                Ok(obj) => obj,
                Err(error) => self.fail(error),
            };

            let message: PyMessage = match Python::attach(|py| {
                obj.extract(py).map_err(|_| {
                    let type_name = obj
                        .bind(py)
                        .get_type()
                        .name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| "<unknown>".to_string());
                    PyErr::new::<PyTypeError, _>(format!(
                        "reducestream handler must yield Message, got {type_name}"
                    ))
                })
            }) {
                Ok(message) => message,
                Err(error) => self.fail(error),
            };

            // The receiver is gone, so the client does not want more messages.
            if output.send(message.into()).await.is_err() {
                break;
            }
        }

        // Ensure forwarder completes
        let _ = forwarder.await;
    }
}

/// Start the reduce stream server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    factory: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting ReduceStream UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    // Shutdown has two sources, and neither one needs a channel here. The Python
    // side signals stop() through shutdown_rx. An uncaught Python error panics in
    // fail(), and numaflow then shuts the server down on its own.
    let creator = PyReduceStreamerCreator {
        event_loop: event_loop.clone(),
        factory: Arc::new(factory),
        errors: Arc::clone(&errors),
    };

    let server = reducestream::Server::new(creator)
        .with_socket_file(sock_file)
        .with_server_info_file(info_file);

    let result = server
        .start_with_shutdown(shutdown_rx)
        .await
        .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()));

    // Ensure the event loop is stopped even if shutdown came from elsewhere.
    Python::attach(|py| {
        if let Ok(stop_cb) = event_loop.getattr(py, "stop") {
            let _ = event_loop.call_method1(py, "call_soon_threadsafe", (stop_cb,));
        }
    });

    println!("Numaflow ReduceStream has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

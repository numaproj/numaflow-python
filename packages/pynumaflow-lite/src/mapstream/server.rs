use std::sync::{Arc, Mutex};

use numaflow::mapstream;
use numaflow::shared::ServerExtras;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::mapstream::Datum;
use crate::mapstream::Message as PyMessage;
use crate::pyiterables::PyAsyncIterStream;
use crate::pyrs::{combine_errors, format_error};

pub(crate) struct PyMapStreamRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl PyMapStreamRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow calls mapstreamer concurrently, so each error belongs to a different
        // message. Keep all of them. start() raises them together, which lets
        // Python format every traceback instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow catches this panic, sends a gRPC error for this message, and
        // starts the server shutdown. An empty result would instead look like a
        // message that the handler dropped on purpose.
        panic!("{message}");
    }
}

#[tonic::async_trait]
impl mapstream::MapStreamer for PyMapStreamRunner {
    async fn map_stream(&self, input: mapstream::MapStreamRequest, tx: Sender<mapstream::Message>) {
        // Call the Python handler: py_func(datum: Datum) -> AsyncIterator[Message]
        let agen = match Python::attach(|py| -> PyResult<_> {
            let datum: Datum = input.into();
            self.py_func.call1(py, (datum,))
        }) {
            Ok(agen) => agen,
            Err(error) => self.fail(error),
        };

        // Wrap the Python AsyncIterable in a Rust Stream that yields incrementally.
        // Items stay as Py<PyAny> so that a wrong item type gives a clear error below.
        let mut stream = match PyAsyncIterStream::<Py<PyAny>>::new(agen, self.event_loop.clone()) {
            Ok(stream) => stream,
            Err(_) => self.fail(PyErr::new::<PyTypeError, _>(
                "mapstream handler must be an async generator (return AsyncIterator[Message])",
            )),
        };

        // Forward each yielded message immediately to the sender
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
                        "mapstream handler must yield Message, got {type_name}"
                    ))
                })
            }) {
                Ok(message) => message,
                Err(error) => self.fail(error),
            };

            // The receiver is gone, so the client does not want more messages.
            if tx.send(message.into()).await.is_err() {
                break;
            }
        }
    }
}

/// Start the mapstream server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_func: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting MapStream UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    // Shutdown has two sources, and neither one needs a channel here. The Python
    // side signals stop() through shutdown_rx. An uncaught Python error panics in
    // fail(), and numaflow then shuts the server down on its own.
    let py_runner = PyMapStreamRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        errors: Arc::clone(&errors),
    };

    let server = numaflow::mapstream::Server::new(py_runner)
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

    println!("Numaflow MapStream has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

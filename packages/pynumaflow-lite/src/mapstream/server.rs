use crate::mapstream::Datum;
use crate::mapstream::Message as PyMessage;
use crate::pyiterables::PyAsyncIterStream;

use numaflow::mapstream;
use numaflow::shared::ServerExtras;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

pub(crate) struct PyMapStreamRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) error_slot: Arc<Mutex<Option<PyErr>>>,
    pub(crate) shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl PyMapStreamRunner {
    fn fail(&self, error: PyErr) {
        let mut error_slot = self.error_slot.lock().unwrap();
        if error_slot.is_none() {
            Python::attach(|py| error.print(py));
            *error_slot = Some(error);
        }
        drop(error_slot);

        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

#[tonic::async_trait]
impl mapstream::MapStreamer for PyMapStreamRunner {
    async fn map_stream(&self, input: mapstream::MapStreamRequest, tx: Sender<mapstream::Message>) {
        // Call Python handler: handler(datum) -> AsyncIterable[Message]
        let agen_obj = match Python::attach(|py| -> PyResult<Py<PyAny>> {
            let datum: Datum = input.into();
            let py_func = self.py_func.clone();
            let agen = py_func.call1(py, (datum,))?;
            if !agen.bind(py).hasattr("__aiter__")? {
                return Err(PyErr::new::<PyTypeError, _>(
                    "mapstream handler must return an async iterable of Message",
                ));
            }
            Ok(agen)
        }) {
            Ok(agen_obj) => agen_obj,
            Err(error) => {
                self.fail(error);
                return;
            }
        };

        // Wrap the Python AsyncIterable in a Rust Stream that yields incrementally
        let mut stream =
            match PyAsyncIterStream::<PyMessage>::new(agen_obj, self.event_loop.clone()) {
                Ok(stream) => stream,
                Err(error) => {
                    self.fail(error);
                    return;
                }
            };

        // Forward each yielded message immediately to the sender
        while let Some(item) = stream.next().await {
            match item {
                Ok(py_msg) => {
                    let out: mapstream::Message = py_msg.into();
                    if tx.send(out).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    self.fail(e);
                    break;
                }
            }
        }
    }
}

/// Start the mapstream server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_func: Py<PyAny>,
    sock_file: String,
    server_info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting MapStream UDF. socket={}, server_info={}",
            &sock_file, &server_info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let error_slot = Arc::new(Mutex::new(None));
    let (internal_shutdown_tx, internal_shutdown_rx) = tokio::sync::oneshot::channel();
    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_rx => {},
            _ = internal_shutdown_rx => {},
        }
        let _ = server_shutdown_tx.send(());
    });

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(server_shutdown_rx);

    let py_runner = PyMapStreamRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        error_slot: error_slot.clone(),
        shutdown_tx: Arc::new(Mutex::new(Some(internal_shutdown_tx))),
    };

    let server = numaflow::mapstream::Server::new(py_runner)
        .with_socket_file(sock_file)
        .with_server_info_file(server_info_file);

    let result = server
        .start_with_shutdown(combined_rx)
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

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        sig_handle.abort();
    }

    if let Some(error) = error_slot.lock().unwrap().take() {
        return Err(error);
    }

    result
}

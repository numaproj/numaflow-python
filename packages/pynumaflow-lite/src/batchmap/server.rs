use std::sync::{Arc, Mutex};

use numaflow::batchmap;
use numaflow::shared::ServerExtras;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::pyrs::{combine_errors, format_error};

pub(crate) struct PyBatchMapRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl PyBatchMapRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow calls batchmap() concurrently, so each error belongs to a different
        // batch. Keep all of them. start() raises them together, which lets
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
impl batchmap::BatchMapper for PyBatchMapRunner {
    async fn batchmap(
        &self,
        mut input: tokio::sync::mpsc::Receiver<batchmap::Datum>,
    ) -> Vec<batchmap::BatchResponse> {
        // Create a channel to stream Datum into Python as an async iterator
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::batchmap::Datum>(64);

        // Spawn a task forwarding incoming datums to the Python-facing channel
        let forwarder = tokio::spawn(async move {
            while let Some(d) = input.recv().await {
                if tx.send(d.into()).await.is_err() {
                    break;
                }
            }
            // When input ends, dropping tx closes the channel
        });

        // Call the Python coroutine: py_func(batch: AsyncIterable[Datum]) -> list[BatchResponse]
        let fut = Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let py_func = self.py_func.clone();

            let stream = crate::batchmap::PyAsyncDatumStream::new_with(rx);
            let coro = py_func.call1(py, (stream,))?.into_bound(py);
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>(
                    "batchmap handler must be an async function (coroutine)",
                )
            })
        });

        let fut = match fut {
            Ok(fut) => fut,
            Err(error) => self.fail(error),
        };

        let result = match fut.await {
            Ok(result) => result,
            Err(error) => self.fail(error),
        };

        // Ensure forwarder completes
        let _ = forwarder.await;

        let responses = Python::attach(|py| {
            result.extract(py).map_err(|_| {
                let type_name = result
                    .bind(py)
                    .get_type()
                    .name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                PyErr::new::<PyTypeError, _>(format!(
                    "batchmap handler must return list[BatchResponse], got {type_name}"
                ))
            })
        });

        let responses: Vec<crate::batchmap::BatchResponse> = match responses {
            Ok(responses) => responses,
            Err(error) => self.fail(error),
        };

        responses
            .into_iter()
            .map(|resp| resp.into())
            .collect::<Vec<batchmap::BatchResponse>>()
    }
}

// Start the batchmap server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_func: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting BatchMap UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_runner = PyBatchMapRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        errors: Arc::clone(&errors),
    };

    let server = numaflow::batchmap::Server::new(py_runner)
        .with_socket_file(sock_file)
        .with_server_info_file(info_file);

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

    println!("Numaflow BatchMap has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        sig_handle.abort();
    }

    result
}

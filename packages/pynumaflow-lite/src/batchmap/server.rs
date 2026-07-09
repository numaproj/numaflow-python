use numaflow::batchmap;
use numaflow::shared::ServerExtras;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

pub(crate) struct PyBatchMapRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) error_slot: Arc<Mutex<Option<PyErr>>>,
    pub(crate) shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl PyBatchMapRunner {
    fn fail(&self, error: PyErr) -> Vec<batchmap::BatchResponse> {
        // Only the first error is reported; later batches may still be in flight
        // while shutdown is underway, and their failures would be duplicates.
        let mut error_slot = self.error_slot.lock().unwrap();
        if error_slot.is_none() {
            Python::attach(|py| error.print(py));
            *error_slot = Some(error);
        }
        drop(error_slot);

        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }

        Vec::new()
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

        // Call the Python coroutine: py_func(batch: AsyncIterator[Datum]) -> list[BatchResponse]
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let py_func = self.py_func.clone();

            let stream = crate::batchmap::PyAsyncDatumStream::new_with(rx);
            let coro = py_func.call1(py, (stream,))?.into_bound(py);
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            if !is_awaitable {
                return Err(PyErr::new::<PyTypeError, _>(
                    "batchmap handler must be an async function (coroutine)",
                ));
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>(
                    "batchmap handler must be an async function (coroutine)",
                )
            })
        }) {
            Ok(fut) => fut,
            Err(error) => return self.fail(error),
        };

        let result = match fut.await {
            Ok(result) => result,
            Err(error) => return self.fail(error),
        };

        // Ensure forwarder completes
        let _ = forwarder.await;

        let responses: Vec<crate::batchmap::BatchResponse> = match Python::attach(|py| {
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
        }) {
            Ok(responses) => responses,
            Err(error) => return self.fail(error),
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
            &sock_file, &info_file
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

    let py_runner = PyBatchMapRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        error_slot: error_slot.clone(),
        shutdown_tx: Arc::new(Mutex::new(Some(internal_shutdown_tx))),
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

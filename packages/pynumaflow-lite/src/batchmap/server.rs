// use crate::batchmap::Datum;
use numaflow::batchmap;
use numaflow::shared::ServerExtras;

use pyo3::prelude::*;
use std::sync::Arc;

pub(crate) struct PyBatchMapRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
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

        // Call the Python coroutine: py_func(batch: AsyncIterable[Datum]) -> BatchResponses
        let fut = Python::attach(|py| {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let py_func = self.py_func.clone();

            let stream = crate::batchmap::PyAsyncDatumStream::new_with(rx);
            let coro = py_func.call1(py, (stream,)).unwrap().into_bound(py);
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).unwrap()
        });

        let result = fut.await.unwrap();

        // Ensure forwarder completes
        let _ = forwarder.await;

        let responses = Python::attach(|py| {
            let x: crate::batchmap::BatchResponses = result.extract(py).unwrap();
            x
        });

        responses
            .responses
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
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    let event_loop = rx.await.unwrap();

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_runner = PyBatchMapRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
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

    println!("Numaflow Core (batch) has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        let _ = sig_handle.abort();
    }

    result
}

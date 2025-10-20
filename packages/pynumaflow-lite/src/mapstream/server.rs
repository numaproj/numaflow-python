use crate::mapstream::Datum;
use crate::mapstream::Message as PyMessage;
use crate::pyiterables::PyAsyncIterStream;

use numaflow::mapstream;
use numaflow::shared::ServerExtras;

use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

pub(crate) struct PyMapStreamRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl mapstream::MapStreamer for PyMapStreamRunner {
    async fn map_stream(&self, input: mapstream::MapStreamRequest, tx: Sender<mapstream::Message>) {
        // Call Python handler: handler(keys, datum) -> AsyncIterator
        let agen_obj = Python::attach(|py| {
            let keys = input.keys.clone();
            let datum: Datum = input.into();
            let py_func = self.py_func.clone();
            let agen = py_func
                .call1(py, (keys, datum))
                .expect("python handler raised before returning async iterable");
            // Keep as Py<PyAny>
            agen.extract(py).unwrap_or(agen)
        });

        // Wrap the Python AsyncIterable in a Rust Stream that yields incrementally
        let mut stream = PyAsyncIterStream::<PyMessage>::new(agen_obj, self.event_loop.clone())
            .expect("failed to construct PyAsyncIterStream");

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
                    // Non-stop errors are surfaced per-item; log and stop this stream.
                    eprintln!("Python async iteration error: {:?}", e);
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
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    let event_loop = rx.await.unwrap();

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_runner = PyMapStreamRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
    };

    let server = numaflow::mapstream::Server::new(py_runner)
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

    println!("Numaflow Core (stream) has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        let _ = sig_handle.abort();
    }

    result
}

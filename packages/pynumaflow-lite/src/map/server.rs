use crate::map::{Datum, Messages};
use numaflow::map;
use numaflow::shared::ServerExtras;

use pyo3::prelude::*;
use std::sync::Arc;

pub(crate) struct PyMapRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl map::Mapper for PyMapRunner {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let fut = Python::attach(|py| {
            let keys = input.keys.clone();
            let input: Datum = input.into();
            let py_func = self.py_func.clone();

            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_func.call1(py, (keys, input)).unwrap().into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro).unwrap()
        });

        let result = fut.await.unwrap();

        let result = Python::attach(|py| {
            let x: Messages = result.extract(py).unwrap();
            x
        });

        println!("{:?}", result);

        result.messages.into_iter().map(|m| m.into()).collect()
    }
}

// Start the map server by spinning up a dedicated Python asyncio loop and wiring shutdown.
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

    let py_map_runner = PyMapRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
    };

    let server = numaflow::map::Server::new(py_map_runner)
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

    println!("Numaflow Core has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        let _ = sig_handle.abort();
    }

    result
}

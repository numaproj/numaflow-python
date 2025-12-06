use crate::sideinput::Response;
use numaflow::sideinput;

use pyo3::prelude::*;
use std::sync::Arc;

pub(crate) struct PySideInputRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_sideinput: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl sideinput::SideInputer for PySideInputRunner {
    async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
        let py_sideinput = self.py_sideinput.clone();
        let event_loop = self.event_loop.clone();

        // Call the Python async retrieve_handler method
        let fut = Python::attach(|py| {
            let locals = pyo3_async_runtimes::TaskLocals::new(event_loop.bind(py).clone());

            let coro = py_sideinput
                .call_method0(py, "retrieve_handler")
                .unwrap()
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro).unwrap()
        });

        let result = fut.await.unwrap();

        let response = Python::attach(|py| {
            let response: Response = result.extract(py).unwrap();
            response
        });

        if response.broadcast {
            Some(response.value)
        } else {
            None
        }
    }
}

// Start the sideinput server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_sideinput: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    let event_loop = rx.await.unwrap();

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_sideinput_runner = PySideInputRunner {
        py_sideinput: Arc::new(py_sideinput),
        event_loop: event_loop.clone(),
    };

    let mut server = sideinput::Server::new(py_sideinput_runner)
        .with_socket_file(&sock_file)
        .with_server_info_file(&info_file);

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

    println!("Numaflow SideInput has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        sig_handle.abort();
    }

    result
}


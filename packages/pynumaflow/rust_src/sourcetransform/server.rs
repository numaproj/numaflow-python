use crate::sourcetransform::{Datum, Messages};
use numaflow::shared::ServerExtras;
use numaflow::sourcetransform;

use pyo3::prelude::*;
use std::sync::Arc;

pub(crate) struct PySourceTransformRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for PySourceTransformRunner {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        // The `numaflow` crate runs each `transform` call inside its own task and
        // converts a panic into a clean `UDF_EXECUTION_ERROR` gRPC status (with a
        // backtrace) before shutting the server down gracefully. The trait returns
        // `Vec<Message>` with no `Result`, so panicking is the only channel for
        // signalling a failed request. We therefore panic with descriptive
        // messages that carry the underlying Python error (exception type and
        // message), rather than using opaque `.unwrap()`s.
        let fut = Python::attach(|py| {
            let keys = input.keys.clone();
            let input: Datum = input.into();
            let py_func = self.py_func.clone();

            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_func
                .call1(py, (keys, input))
                .unwrap_or_else(|err| panic!("calling the Python UDF raised an error: {err}"))
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro).unwrap_or_else(|err| {
                panic!("the Python UDF did not return an awaitable coroutine: {err}")
            })
        });

        let result = fut
            .await
            .unwrap_or_else(|err| panic!("awaiting the Python UDF raised an error: {err}"));

        let result = Python::attach(|py| {
            let messages: Messages = result.extract(py).unwrap_or_else(|err| {
                panic!("the Python UDF did not return a valid Messages object: {err}")
            });
            messages
        });

        result.messages.into_iter().map(|m| m.into()).collect()
    }
}

// Start the sourcetransform server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_func: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    // `run_asyncio` sends back either the created event loop or the `PyErr`
    // raised while creating it. A `RecvError` here means the loop thread died
    // before sending anything (e.g. it panicked).
    let event_loop = match rx.await {
        Ok(Ok(event_loop)) => event_loop,
        Ok(Err(err)) => return Err(err),
        Err(_) => {
            return Err(pyo3::PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "asyncio event loop thread terminated before the loop was created",
            ));
        }
    };

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_sourcetransform_runner = PySourceTransformRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
    };

    let server = numaflow::sourcetransform::Server::new(py_sourcetransform_runner)
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

    println!("Numaflow SourceTransform has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        sig_handle.abort();
    }

    result
}

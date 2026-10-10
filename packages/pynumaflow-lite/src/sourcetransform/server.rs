use std::sync::{Arc, Mutex};

use numaflow::shared::ServerExtras;
use numaflow::sourcetransform;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::pyrs::{combine_errors, format_error};

pub(crate) struct PySourceTransformRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl PySourceTransformRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow calls transform() concurrently, so each error belongs to a
        // different message. Keep all of them. start() raises them together, which
        // lets Python format every traceback instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow catches this panic, sends a gRPC error for this message, and
        // starts the server shutdown. An empty result would instead look like a
        // message that the handler dropped on purpose.
        panic!("{message}");
    }
}

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for PySourceTransformRunner {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let datum: crate::sourcetransform::Datum = input.into();
            let coro = self.py_func.call1(py, (datum,))?.into_bound(py);
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            if !is_awaitable {
                return Err(PyErr::new::<PyTypeError, _>(
                    "sourcetransform handler must be an async function (coroutine)",
                ));
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>(
                    "sourcetransform handler must be an async function (coroutine)",
                )
            })
        }) {
            Ok(fut) => fut,
            Err(error) => self.fail(error),
        };

        let result = match fut.await {
            Ok(result) => result,
            Err(error) => self.fail(error),
        };

        let messages: Vec<crate::sourcetransform::Message> = match Python::attach(|py| {
            result.extract(py).map_err(|_| {
                let type_name = result
                    .bind(py)
                    .get_type()
                    .name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                PyErr::new::<PyTypeError, _>(format!(
                    "sourcetransform handler must return list[Message], got {type_name}"
                ))
            })
        }) {
            Ok(messages) => messages,
            Err(error) => self.fail(error),
        };

        messages.into_iter().map(|m| m.into()).collect()
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
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting SourceTransform UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    // Shutdown has two sources, and neither one needs a channel here. The Python
    // side signals stop() through shutdown_rx. An uncaught Python error panics in
    // fail(), and numaflow then shuts the server down on its own.
    let py_sourcetransform_runner = PySourceTransformRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        errors: Arc::clone(&errors),
    };

    let server = numaflow::sourcetransform::Server::new(py_sourcetransform_runner)
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

    println!("Numaflow SourceTransform has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

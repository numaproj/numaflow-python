use std::sync::{Arc, Mutex};

use crate::reduce::{
    Datum as PyDatum, IntervalWindow as PyIntervalWindow, Message as PyMessage,
    Metadata as PyMetadata, PyAsyncDatumStream,
};
use numaflow::reduce;
use numaflow::shared::ServerExtras;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::pyrs::{combine_errors, format_error};

pub(crate) struct PyReduceCreator {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Zero-argument Python callable that returns the handler for one window
    pub(crate) factory: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

pub(crate) struct PyReduceRunner {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) factory: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl reduce::ReducerCreator for PyReduceCreator {
    type R = PyReduceRunner;

    // The factory runs later, in reduce(). A panic in create() would stop the numaflow
    // task executor without an error to the client, but reduce() reports a panic.
    fn create(&self) -> Self::R {
        PyReduceRunner {
            event_loop: self.event_loop.clone(),
            factory: self.factory.clone(),
            errors: self.errors.clone(),
        }
    }
}

impl PyReduceRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow calls reduce() concurrently, so each error belongs to a
        // different window. Keep all of them. start() raises them together, which
        // lets Python format every traceback instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow catches this panic, sends a gRPC error for this window, and
        // starts the server shutdown. An empty result would instead look like a
        // window that the handler reduced to no messages on purpose.
        panic!("{message}");
    }
}

#[tonic::async_trait]
impl reduce::Reducer for PyReduceRunner {
    async fn reduce(
        &self,
        keys: Vec<String>,
        mut input: tokio::sync::mpsc::Receiver<reduce::ReduceRequest>,
        md: &reduce::Metadata,
    ) -> Vec<reduce::Message> {
        // Create a channel to stream Datum into Python as an async iterator
        let (tx, rx) = tokio::sync::mpsc::channel::<PyDatum>(64);

        // Spawn a task forwarding incoming datums to the Python-facing channel
        let forwarder = tokio::spawn(async move {
            while let Some(req) = input.recv().await {
                if tx.send(req.into()).await.is_err() {
                    break;
                }
            }
        });

        // Call the Python coroutine:
        // factory()(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> list[Message]
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let handler = self.factory.call0(py)?;

            let stream = PyAsyncDatumStream::new_with(rx);
            let interval =
                PyIntervalWindow::new(md.interval_window.start_time, md.interval_window.end_time);
            let md_py = PyMetadata::new(interval);

            let coro = handler.call1(py, (keys, stream, md_py))?.into_bound(py);
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            if !is_awaitable {
                return Err(PyErr::new::<PyTypeError, _>(
                    "reduce handler must be an async function (coroutine)",
                ));
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>("reduce handler must be an async function (coroutine)")
            })
        }) {
            Ok(fut) => fut,
            Err(error) => self.fail(error),
        };

        let result = match fut.await {
            Ok(result) => result,
            Err(error) => self.fail(error),
        };

        // Ensure forwarder completes
        let _ = forwarder.await;

        let messages: Vec<PyMessage> = match Python::attach(|py| {
            result.extract(py).map_err(|_| {
                let type_name = result
                    .bind(py)
                    .get_type()
                    .name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                PyErr::new::<PyTypeError, _>(format!(
                    "reduce handler must return list[Message], got {type_name}"
                ))
            })
        }) {
            Ok(messages) => messages,
            Err(error) => self.fail(error),
        };

        messages.into_iter().map(|m| m.into()).collect()
    }
}

/// Start the reduce server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    factory: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting Reduce UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    // Shutdown has two sources, and neither one needs a channel here. The Python
    // side signals stop() through shutdown_rx. An uncaught Python error panics in
    // fail(), and numaflow then shuts the server down on its own.
    let creator = PyReduceCreator {
        event_loop: event_loop.clone(),
        factory: Arc::new(factory),
        errors: Arc::clone(&errors),
    };

    let server = reduce::Server::new(creator)
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

    println!("Numaflow Reduce has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

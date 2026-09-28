use std::sync::{Arc, Mutex};

use numaflow::shared::ServerExtras;
use numaflow::sink;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::pyrs::{combine_errors, format_error};

pub(crate) struct PySinkRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
}

impl PySinkRunner {
    fn fail(&self, error: PyErr) -> ! {
        // Keep every error. start() raises them together, which lets Python
        // format every traceback instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow catches this panic, sends a gRPC error for this batch, and
        // starts the server shutdown. An empty result would instead look like a
        // batch that the handler wrote with no responses.
        panic!("{message}");
    }
}

#[tonic::async_trait]
impl sink::Sinker for PySinkRunner {
    async fn sink(
        &self,
        mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
    ) -> Vec<sink::Response> {
        // Create a channel to stream Datum into Python as an async iterator
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::sink::Datum>(64);

        // Spawn a task forwarding incoming datums to the Python-facing channel
        let forwarder = tokio::spawn(async move {
            while let Some(req) = input.recv().await {
                if tx.send(req.into()).await.is_err() {
                    break;
                }
            }
            // When input ends, dropping tx closes the channel
        });

        // Call the Python coroutine: py_func(datums: AsyncIterator[Datum]) -> list[Response]
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let py_func = self.py_func.clone();

            let stream = crate::sink::PyAsyncDatumStream::new_with(rx);
            let coro = py_func.call1(py, (stream,))?.into_bound(py);
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            if !is_awaitable {
                return Err(PyErr::new::<PyTypeError, _>(
                    "sink handler must be an async function (coroutine)",
                ));
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>("sink handler must be an async function (coroutine)")
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

        let responses: Vec<crate::sink::Response> = match Python::attach(|py| {
            result.extract(py).map_err(|_| {
                let type_name = result
                    .bind(py)
                    .get_type()
                    .name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                PyErr::new::<PyTypeError, _>(format!(
                    "sink handler must return list[Response], got {type_name}"
                ))
            })
        }) {
            Ok(responses) => responses,
            Err(error) => self.fail(error),
        };

        responses
            .into_iter()
            .map(|resp| resp.into())
            .collect::<Vec<sink::Response>>()
    }
}

/// Start the sink server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_func: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting Sink UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));

    // Shutdown has two sources, and neither one needs a channel here. The Python
    // side signals stop() through shutdown_rx. An uncaught Python error panics in
    // fail(), and numaflow then shuts the server down on its own.
    let py_runner = PySinkRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        errors: Arc::clone(&errors),
    };

    let server = numaflow::sink::Server::new(py_runner)
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

    println!("Numaflow Sink has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

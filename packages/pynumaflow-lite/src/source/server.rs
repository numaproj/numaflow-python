use std::sync::{Arc, Mutex};

use numaflow::shared::ServerExtras;
use numaflow::source;
use pyo3::call::PyCallArgs;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use tokio::sync::Notify;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

use crate::pyiterables::PyAsyncIterStream;
use crate::pyrs::{combine_errors, format_error};
use crate::source::{Message, NackOffset, PyOffset, ReadRequest};

pub(crate) struct PySourceRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) sourcer: Arc<Py<PyAny>>,
    pub(crate) errors: Arc<Mutex<Vec<PyErr>>>,
    /// Notified when a handler fails, so that start() shuts the server down.
    pub(crate) failed: Arc<Notify>,
}

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "<unknown>".to_string())
}

impl PySourceRunner {
    fn fail(&self, error: PyErr) -> ! {
        // numaflow can call the handlers concurrently, so keep every error.
        // start() raises them together, which lets Python format every traceback
        // instead of Rust printing them by hand.
        let message = Python::attach(|py| format_error(py, &error));
        self.errors.lock().unwrap().push(error);

        // numaflow shuts the server down after a panic in read or ack, but not
        // after one in nack, pending, or active_partitions. Ask for the shutdown here so
        // that every handler failure stops the server in the same way.
        self.failed.notify_one();

        // The panic makes the client get a gRPC error. Returning a value instead
        // would look like a valid answer from the handler.
        panic!("{message}");
    }

    /// Call `sourcer.<method>(*args)`, await the coroutine, and return its result.
    async fn call<A>(&self, method: &str, args: A) -> Py<PyAny>
    where
        A: for<'py> PyCallArgs<'py> + Send,
    {
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let coro = self.sourcer.bind(py).call_method1(method, args)?;
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            let not_async = || {
                PyErr::new::<PyTypeError, _>(format!(
                    "source {method} must be an async function (coroutine)"
                ))
            };
            if !is_awaitable {
                return Err(not_async());
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| not_async())
        }) {
            Ok(fut) => fut,
            Err(error) => self.fail(error),
        };

        match fut.await {
            Ok(result) => result,
            Err(error) => self.fail(error),
        }
    }

    /// Like `call`, then convert the result to `T`, or fail with a TypeError that
    /// names `expected`.
    async fn call_and_extract<T, A>(&self, method: &str, args: A, expected: &str) -> T
    where
        T: for<'a, 'py> FromPyObject<'a, 'py>,
        A: for<'py> PyCallArgs<'py> + Send,
    {
        let result = self.call(method, args).await;
        match Python::attach(|py| {
            let result = result.bind(py);
            result.extract::<T>().map_err(|_| {
                PyErr::new::<PyTypeError, _>(format!(
                    "source {method} must return {expected}, got {}",
                    type_name(result)
                ))
            })
        }) {
            Ok(value) => value,
            Err(error) => self.fail(error),
        }
    }
}

#[tonic::async_trait]
impl source::Sourcer for PySourceRunner {
    async fn read(&self, request: source::SourceReadRequest, transmitter: Sender<source::Message>) {
        let request: ReadRequest = request.into();

        // sourcer.read(request) -> AsyncIterator[Message]
        let agen = match Python::attach(|py| self.sourcer.call_method1(py, "read", (request,))) {
            Ok(agen) => agen,
            Err(error) => self.fail(error),
        };

        // Items stay as Py<PyAny> so that a wrong item type gives a clear error below.
        let agen_ref = Python::attach(|py| agen.clone_ref(py));
        let mut stream =
            match PyAsyncIterStream::<Py<PyAny>>::new(agen_ref, self.event_loop.clone()) {
                Ok(stream) => stream,
                Err(_) => {
                    // A coroutine (async def without yield) must be closed, or Python
                    // warns that it was never awaited.
                    Python::attach(|py| {
                        let _ = agen.call_method0(py, "close");
                    });
                    self.fail(PyErr::new::<PyTypeError, _>(
                        "source read must be an async generator (yield Message)",
                    ))
                }
            };

        while let Some(item) = stream.next().await {
            let obj = match item {
                Ok(obj) => obj,
                Err(error) => self.fail(error),
            };

            let message: Message = match Python::attach(|py| {
                let obj = obj.bind(py);
                obj.extract().map_err(|_| {
                    PyErr::new::<PyTypeError, _>(format!(
                        "source read must yield Message, got {}",
                        type_name(obj)
                    ))
                })
            }) {
                Ok(message) => message,
                Err(error) => self.fail(error),
            };

            // The receiver is gone, so the client does not want more messages.
            if transmitter.send(message.into()).await.is_err() {
                break;
            }
        }
    }

    async fn ack(&self, offsets: Vec<source::Offset>) {
        let offsets: Vec<PyOffset> = offsets.into_iter().map(Into::into).collect();
        self.call("ack", (offsets,)).await;
    }

    async fn nack(&self, offsets: Vec<source::NackOffset>) {
        let offsets: Vec<NackOffset> = offsets.into_iter().map(Into::into).collect();
        self.call("nack", (offsets,)).await;
    }

    async fn pending(&self) -> Option<usize> {
        match self
            .call_and_extract::<Option<i64>, _>("pending", (), "int or None")
            .await
        {
            None => None,
            Some(count) => match usize::try_from(count) {
                Ok(count) => Some(count),
                Err(_) => self.fail(PyErr::new::<PyValueError, _>(format!(
                    "source pending must not be negative, got {count}; return None when the count is unknown"
                ))),
            },
        }
    }

    async fn active_partitions(&self) -> Option<Vec<i32>> {
        self.call_and_extract("active_partitions", (), "list[int] or None")
            .await
    }

    async fn total_partitions(&self) -> Option<i32> {
        self.call_and_extract("total_partitions", (), "int or None")
            .await
    }
}

/// Start the source server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    sourcer: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting Source UDF. socket={}, server_info={}",
            sock_file, info_file
        );
        move || crate::pyrs::run_asyncio(tx)
    });
    let event_loop = rx.await.unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));
    let failed = Arc::new(Notify::new());

    // Shutdown has two sources: stop() from Python through shutdown_rx, and a
    // handler failure through `failed`.
    let (server_shutdown_tx, server_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown_watcher = tokio::spawn({
        let failed = Arc::clone(&failed);
        async move {
            tokio::select! {
                _ = shutdown_rx => {},
                _ = failed.notified() => {},
            }
            let _ = server_shutdown_tx.send(());
        }
    });

    let py_source_runner = PySourceRunner {
        sourcer: Arc::new(sourcer),
        event_loop: event_loop.clone(),
        errors: Arc::clone(&errors),
        failed,
    };

    let server = numaflow::source::Server::new(py_source_runner)
        .with_socket_file(sock_file)
        .with_server_info_file(info_file);

    let result = server
        .start_with_shutdown(server_shutdown_rx)
        .await
        .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()));

    shutdown_watcher.abort();

    // Ensure the event loop is stopped even if shutdown came from elsewhere.
    Python::attach(|py| {
        if let Ok(stop_cb) = event_loop.getattr(py, "stop") {
            let _ = event_loop.call_method1(py, "call_soon_threadsafe", (stop_cb,));
        }
    });

    println!("Numaflow Source has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    let errors = std::mem::take(&mut *errors.lock().unwrap());
    if !errors.is_empty() {
        return Err(Python::attach(|py| combine_errors(py, errors)));
    }

    result
}

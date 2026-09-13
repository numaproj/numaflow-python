use numaflow::map;
use numaflow::shared::ServerExtras;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

pub(crate) struct PyMapRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_func: Arc<Py<PyAny>>,
    pub(crate) error_slot: Arc<Mutex<Option<PyErr>>>,
    pub(crate) shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl PyMapRunner {
    fn fail(&self, error: PyErr) -> Vec<map::Message> {
        // Only the first error is reported; later requests may still be in flight
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
impl map::Mapper for PyMapRunner {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let fut = match Python::attach(|py| -> PyResult<_> {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let datum: crate::map::Datum = input.into();
            let coro = self.py_func.call1(py, (datum,))?.into_bound(py);
            let is_awaitable: bool = py
                .import("inspect")?
                .call_method1("isawaitable", (&coro,))?
                .extract()?;
            if !is_awaitable {
                return Err(PyErr::new::<PyTypeError, _>(
                    "map handler must be an async function (coroutine)",
                ));
            }
            pyo3_async_runtimes::into_future_with_locals(&locals, coro).map_err(|_| {
                PyErr::new::<PyTypeError, _>("map handler must be an async function (coroutine)")
            })
        }) {
            Ok(fut) => fut,
            Err(error) => return self.fail(error),
        };

        let result = match fut.await {
            Ok(result) => result,
            Err(error) => return self.fail(error),
        };

        let messages: Vec<crate::map::Message> = match Python::attach(|py| {
            result.extract(py).map_err(|_| {
                let type_name = result
                    .bind(py)
                    .get_type()
                    .name()
                    .map(|name| name.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "<unknown>".to_string());
                PyErr::new::<PyTypeError, _>(format!(
                    "map handler must return list[Message], got {type_name}"
                ))
            })
        }) {
            Ok(messages) => messages,
            Err(error) => return self.fail(error),
        };

        messages.into_iter().map(|m| m.into()).collect()
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
    let py_asyncio_loop_handle = tokio::task::spawn_blocking({
        println!(
            "Starting Map UDF. socket={}, server_info={}",
            sock_file, info_file
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

    // The Python wrapper owns OS signal handling and drives shutdown via stop().
    let py_map_runner = PyMapRunner {
        py_func: Arc::new(py_func),
        event_loop: event_loop.clone(),
        error_slot: error_slot.clone(),
        shutdown_tx: Arc::new(Mutex::new(Some(internal_shutdown_tx))),
    };

    let server = numaflow::map::Server::new(py_map_runner)
        .with_socket_file(sock_file)
        .with_server_info_file(info_file);

    let result = server
        .start_with_shutdown(server_shutdown_rx)
        .await
        .map_err(|e| pyo3::PyErr::new::<pyo3::exceptions::PyException, _>(e.to_string()));

    // Ensure the event loop is stopped even if shutdown came from elsewhere.
    Python::attach(|py| {
        if let Ok(stop_cb) = event_loop.getattr(py, "stop") {
            let _ = event_loop.call_method1(py, "call_soon_threadsafe", (stop_cb,));
        }
    });

    println!("Numaflow Map has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    if let Some(error) = error_slot.lock().unwrap().take() {
        return Err(error);
    }

    result
}

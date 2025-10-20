use crate::reduce::{
    Datum as PyDatum, IntervalWindow as PyIntervalWindow, Message as PyMessage,
    Messages as PyMessages, Metadata as PyMetadata, PyAsyncDatumStream,
};
use numaflow::reduce;
use numaflow::shared::ServerExtras;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::sync::Arc;

pub(crate) struct PyReduceCreator {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Python class to instantiate per window OR a function
    pub(crate) py_creator: Arc<Py<PyAny>>,
    /// optional tuple of positional args (only for class)
    pub(crate) init_args: Option<Arc<Py<PyAny>>>,
    /// true if py_creator is a function, false if it's a class.
    pub(crate) is_function: bool,
}

pub(crate) struct PyReduceRunner {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Instance of class per window OR the function itself. It can be class because we have overloaded
    /// __call__ method to make it callable like a function.
    pub(crate) py_obj: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl reduce::ReducerCreator for PyReduceCreator {
    type R = PyReduceRunner;

    fn create(&self) -> Self::R {
        // If it's a function, just clone the reference; if it's a class, instantiate it.
        let inst = if self.is_function {
            // For functions, we don't instantiate; just use the function directly
            Python::attach(|py| self.py_creator.clone_ref(py))
        } else {
            // For classes, instantiate the Python class synchronously under the GIL.
            Python::attach(|py| {
                let class = self.py_creator.as_ref();
                match &self.init_args {
                    Some(args) => {
                        let bound = args.as_ref().bind(py);
                        let py_tuple = bound.downcast::<PyTuple>()?;
                        class.call1(py, py_tuple)
                    }
                    None => class.call0(py),
                }
            })
            .expect("failed to instantiate Python reducer class")
        };

        PyReduceRunner {
            event_loop: self.event_loop.clone(),
            py_obj: Arc::new(inst),
        }
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
                let datum = PyDatum::from(req);
                if tx.send(datum).await.is_err() {
                    break;
                }
            }
        });

        // Call the Python coroutine:
        // - For class-based: obj.handler(keys, datums: AsyncIterable[Datum], md: Metadata) -> Messages
        // - For function-based: obj(keys, datums: AsyncIterable[Datum], md: Metadata) -> Messages
        let fut = Python::attach(|py| {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let obj = self.py_obj.clone();

            // Build metadata and stream objects
            let stream = PyAsyncDatumStream::new_with(rx);
            let interval =
                PyIntervalWindow::new(md.interval_window.start_time, md.interval_window.end_time);
            let md_py = PyMetadata::new(interval);

            // doesn't matter whether it is func or class, obj is callable
            let coro = obj
                .call1(py, (keys.clone(), stream, md_py))
                .unwrap()
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro).unwrap()
        });

        let result = fut.await.unwrap();

        // Ensure forwarder completes
        let _ = forwarder.await;

        let messages = Python::attach(|py| {
            // Expect Messages; also allow a single Message for convenience
            if let Ok(msgs) = result.extract::<PyMessages>(py) {
                msgs.messages
                    .into_iter()
                    .map(|m| reduce::Message::from(m))
                    .collect::<Vec<reduce::Message>>()
            } else if let Ok(single) = result.extract::<PyMessage>(py) {
                vec![single.into()]
            } else {
                vec![]
            }
        });

        messages
    }
}

/// Start the reduce server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_creator: Py<PyAny>,
    init_args: Option<Py<PyAny>>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    let event_loop = rx.await.unwrap();

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    // Detect if py_creator is a function or a class
    let is_function = Python::attach(|py| {
        let obj = py_creator.bind(py);
        // Check if it's a function or coroutine function using inspect module
        let inspect = py.import("inspect").ok()?;
        if let Ok(is_func) = inspect.call_method1("isfunction", (obj,)) {
            if let Ok(result) = is_func.extract::<bool>() {
                if result {
                    return Some(true);
                }
            }
        }
        // Also check for coroutine function
        if let Ok(is_coro) = inspect.call_method1("iscoroutinefunction", (obj,)) {
            if let Ok(result) = is_coro.extract::<bool>() {
                if result {
                    return Some(true);
                }
            }
        }
        Some(false)
    })
    .unwrap_or(false);

    let creator = PyReduceCreator {
        event_loop: event_loop.clone(),
        py_creator: Arc::new(py_creator),
        init_args: init_args.map(Arc::new),
        is_function,
    };

    let server = reduce::Server::new(creator)
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

    println!("Numaflow Core (reduce) has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        let _ = sig_handle.abort();
    }

    result
}

use crate::pyiterables::PyAsyncIterStream;
use crate::session_reduce::{Datum as PyDatum, Message as PyMessage, PyAsyncDatumStream};
use numaflow::session_reduce;
use numaflow::shared::ServerExtras;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::sync::Arc;
use tokio_stream::StreamExt;

pub(crate) struct PySessionReduceCreator {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Python class to instantiate per window
    pub(crate) py_creator: Arc<Py<PyAny>>,
    /// optional tuple of positional args
    pub(crate) init_args: Option<Arc<Py<PyAny>>>,
}

pub(crate) struct PySessionReduceRunner {
    /// handle to Python event loop
    pub(crate) event_loop: Arc<Py<PyAny>>,
    /// Instance of class per window
    pub(crate) py_instance: Arc<Py<PyAny>>,
}

impl session_reduce::SessionReducerCreator for PySessionReduceCreator {
    type R = PySessionReduceRunner;

    fn create(&self) -> Self::R {
        // Instantiate the Python class synchronously under the GIL.
        let inst = Python::attach(|py| {
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
        .expect("failed to instantiate Python session reducer class");

        PySessionReduceRunner {
            event_loop: self.event_loop.clone(),
            py_instance: Arc::new(inst),
        }
    }
}

#[tonic::async_trait]
impl session_reduce::SessionReducer for PySessionReduceRunner {
    async fn session_reduce(
        &self,
        keys: Vec<String>,
        mut input: tokio::sync::mpsc::Receiver<session_reduce::SessionReduceRequest>,
        output: tokio::sync::mpsc::Sender<session_reduce::Message>,
    ) {
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
            // When input ends, dropping tx closes the channel and that is when
            // the Python async iterable will stop.
        });

        // Call the Python coroutine:
        // obj.session_reduce(keys, datums: AsyncIterable[Datum]) -> AsyncIterator[Message]
        let agen_obj = Python::attach(|py| {
            let obj = self.py_instance.clone();
            let stream = PyAsyncDatumStream::new_with(rx);

            // Call session_reduce method
            let agen = obj
                .call_method1(py, "session_reduce", (keys.clone(), stream))
                .expect("python session_reduce method raised before returning async iterable");

            // Keep as Py<PyAny>
            agen.extract(py).unwrap_or(agen)
        });

        // Wrap the Python AsyncIterable in a Rust Stream that yields incrementally
        let mut stream = PyAsyncIterStream::<PyMessage>::new(agen_obj, self.event_loop.clone())
            .expect("failed to construct PyAsyncIterStream");

        // Forward each yielded message immediately to the output sender
        while let Some(item) = stream.next().await {
            match item {
                Ok(py_msg) => {
                    let out: session_reduce::Message = py_msg.into();
                    if output.send(out).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    // Non-stop errors are surfaced per-item; log and stop this stream.
                    eprintln!("Python async iteration error in session_reduce: {:?}", e);
                    break;
                }
            }
        }

        // Ensure forwarder completes
        let _ = forwarder.await;
    }

    async fn accumulator(&self) -> Vec<u8> {
        // Call Python's accumulator() async method
        let fut = Python::attach(|py| {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let obj = self.py_instance.clone();

            let coro = obj
                .call_method0(py, "accumulator")
                .expect("python accumulator method raised")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to create future for accumulator")
        });

        let result = fut.await.expect("accumulator future failed");

        Python::attach(|py| {
            result
                .extract::<Vec<u8>>(py)
                .expect("accumulator must return bytes")
        })
    }

    async fn merge_accumulator(&self, accumulator: Vec<u8>) {
        // Call Python's merge_accumulator(accumulator: bytes) async method
        let fut = Python::attach(|py| {
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());
            let obj = self.py_instance.clone();

            let coro = obj
                .call_method1(py, "merge_accumulator", (accumulator,))
                .expect("python merge_accumulator method raised")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to create future for merge_accumulator")
        });

        let _ = fut.await.expect("merge_accumulator future failed");
    }
}

/// Start the session reduce server by spinning up a dedicated Python asyncio loop and wiring shutdown.
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

    let creator = PySessionReduceCreator {
        event_loop: event_loop.clone(),
        py_creator: Arc::new(py_creator),
        init_args: init_args.map(Arc::new),
    };

    let server = session_reduce::Server::new(creator)
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

    println!("Numaflow Core (session_reduce) has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        let _ = sig_handle.abort();
    }

    result
}

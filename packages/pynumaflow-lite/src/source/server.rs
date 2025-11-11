use numaflow::shared::ServerExtras;

use pyo3::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub(crate) struct PySourceRunner {
    pub(crate) event_loop: Arc<Py<PyAny>>,
    pub(crate) py_handler: Arc<Py<PyAny>>,
}

#[tonic::async_trait]
impl numaflow::source::Sourcer for PySourceRunner {
    /// Reads the messages from the source and sends them to the transmitter.
    async fn read(
        &self,
        request: numaflow::source::SourceReadRequest,
        transmitter: Sender<numaflow::source::Message>,
    ) {
        // Convert the Rust request to Python ReadRequest
        let read_request: crate::source::ReadRequest = request.into();

        // Call the Python read_handler which returns an AsyncIterator[Message]
        let py_async_iter = Python::attach(|py| {
            let py_handler = self.py_handler.clone();

            // Call read_handler(request) -> AsyncIterator[Message]
            let result = py_handler
                .call_method1(py, "read_handler", (read_request,))
                .expect("failed to call read_handler");

            result
        });

        // Create a stream from the Python AsyncIterator
        let mut stream = crate::pyiterables::PyAsyncIterStream::<crate::source::Message>::new(
            py_async_iter,
            self.event_loop.clone(),
        )
        .expect("failed to create stream from Python AsyncIterator");

        // Stream messages from Python to the transmitter
        use tokio_stream::StreamExt;
        while let Some(result) = stream.next().await {
            match result {
                Ok(py_message) => {
                    let rust_message: numaflow::source::Message = py_message.into();
                    if transmitter.send(rust_message).await.is_err() {
                        // Receiver dropped, stop reading
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from Python source: {:?}", e);
                    break;
                }
            }
        }
    }

    /// Acknowledges the message that has been processed by the user-defined source.
    async fn ack(&self, offsets: Vec<numaflow::source::Offset>) {
        // Convert Rust offsets to Python Offset objects
        let py_offsets: Vec<crate::source::PyOffset> =
            offsets.into_iter().map(|o| o.into()).collect();

        // Create AckRequest
        let ack_request = crate::source::AckRequest::new(py_offsets);

        // Call the Python ack_handler
        let fut = Python::attach(|py| {
            let py_handler = self.py_handler.clone();
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_handler
                .call_method1(py, "ack_handler", (ack_request,))
                .expect("failed to call ack_handler")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to convert ack_handler to future")
        });

        // Await the Python coroutine
        let _ = fut.await;
    }

    /// Negatively acknowledges the message that has been processed by the user-defined source.
    async fn nack(&self, offsets: Vec<numaflow::source::Offset>) {
        // Convert Rust offsets to Python Offset objects
        let py_offsets: Vec<crate::source::PyOffset> =
            offsets.into_iter().map(|o| o.into()).collect();

        // Create NackRequest
        let nack_request = crate::source::NackRequest::new(py_offsets);

        // Call the Python nack_handler
        let fut = Python::attach(|py| {
            let py_handler = self.py_handler.clone();
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_handler
                .call_method1(py, "nack_handler", (nack_request,))
                .expect("failed to call nack_handler")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to convert nack_handler to future")
        });

        // Await the Python coroutine
        let _ = fut.await;
    }

    /// Returns the number of messages that are yet to be processed by the user-defined source.
    /// The None value can be returned if source doesn't support detecting the backlog.
    async fn pending(&self) -> Option<usize> {
        // Call the Python pending_handler
        let fut = Python::attach(|py| {
            let py_handler = self.py_handler.clone();
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_handler
                .call_method0(py, "pending_handler")
                .expect("failed to call pending_handler")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to convert pending_handler to future")
        });

        // Await the Python coroutine and extract the result
        let result = fut.await.expect("pending_handler failed");

        let pending_response = Python::attach(|py| {
            result
                .extract::<crate::source::PendingResponse>(py)
                .expect("failed to extract PendingResponse")
        });

        // Convert count to Option<usize>
        // -1 means the source doesn't support detecting backlog
        if pending_response.count < 0 {
            None
        } else {
            Some(pending_response.count as usize)
        }
    }

    /// Returns the partitions associated with the source. This will be used by the platform to determine
    /// the partitions to which the watermark should be published. Some sources might not have the concept of partitions.
    /// Kafka is an example of source where a reader can read from multiple partitions.
    /// If None is returned, Numaflow replica-id will be returned as the partition.
    async fn partitions(&self) -> Option<Vec<i32>> {
        // Call the Python partitions_handler
        let fut = Python::attach(|py| {
            let py_handler = self.py_handler.clone();
            let locals = pyo3_async_runtimes::TaskLocals::new(self.event_loop.bind(py).clone());

            let coro = py_handler
                .call_method0(py, "partitions_handler")
                .expect("failed to call partitions_handler")
                .into_bound(py);

            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
                .expect("failed to convert partitions_handler to future")
        });

        // Await the Python coroutine and extract the result
        let result = fut.await.expect("partitions_handler failed");

        let partitions_response = Python::attach(|py| {
            result
                .extract::<crate::source::PartitionsResponse>(py)
                .expect("failed to extract PartitionsResponse")
        });

        Some(partitions_response.partitions)
    }
}

/// Start the source server by spinning up a dedicated Python asyncio loop and wiring shutdown.
pub(super) async fn start(
    py_handler: Py<PyAny>,
    sock_file: String,
    info_file: String,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), pyo3::PyErr> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let py_asyncio_loop_handle = tokio::task::spawn_blocking(move || crate::pyrs::run_asyncio(tx));
    let event_loop = rx.await.unwrap();

    let (sig_handle, combined_rx) = crate::pyrs::setup_sig_handler(shutdown_rx);

    let py_source_runner = PySourceRunner {
        py_handler: Arc::new(py_handler),
        event_loop: event_loop.clone(),
    };

    let server = numaflow::source::Server::new(py_source_runner)
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

    println!("Numaflow Source has shutdown...");

    // Wait for the blocking asyncio thread to finish.
    let _ = py_asyncio_loop_handle.await;

    // if not finished, abort it
    if !sig_handle.is_finished() {
        println!("Aborting signal handler");
        sig_handle.abort();
    }

    result
}

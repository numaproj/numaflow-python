//! Convert Python AsyncIterator into Rust Stream and Rust channel into Python AsyncIterator.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use pin_project::pin_project;
use pyo3::{PyClass, exceptions::PyStopAsyncIteration, prelude::*};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

/// Stream over a Python AsyncIterator, yielding `M` as soon as each value is produced.
/// `M` must be extractable from the Python object.
///
/// Keep a handle to the target asyncio event loop and await each `__anext__` using
/// pyo3-async-runtimes so items are yielded incrementally without buffering.
#[pin_project]
pub struct PyAsyncIterStream<M> {
    // The Python object that is the async-iterator (result of __aiter__()).
    aiter: Py<PyAny>,
    // The asyncio event loop this stream should use.
    event_loop: Arc<Py<PyAny>>,
    // In-flight future for the next item (converted from Python awaitable).
    #[pin]
    next_fut: Option<Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send + 'static>>>,
    // Phantom: we yield M
    _marker: std::marker::PhantomData<M>,
}

impl<M> PyAsyncIterStream<M>
where
    M: for<'py> FromPyObject<'py> + Send + 'static,
{
    /// Given a Python AsyncIterator and the event loop, build a stream over its items.
    /// It calls `__aiter__` on the `async_iterable` to get the iterator.
    pub fn new(async_iterable: Py<PyAny>, event_loop: Arc<Py<PyAny>>) -> PyResult<Self> {
        let aiter = Python::attach(|py| async_iterable.call_method0(py, "__aiter__"))?;
        Ok(Self {
            aiter,
            event_loop,
            next_fut: None,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<M> Stream for PyAsyncIterStream<M>
where
    M: for<'py> FromPyObject<'py> + Send + 'static,
{
    type Item = PyResult<M>;

    /// Polls the next item from the Python AsyncIterable.
    ///
    /// Overview
    /// - Lazily creates a new Python `__anext__()` awaitable when there is no in-flight
    ///   future and binds it to the target asyncio event loop via pyo3-async-runtimes.
    /// - Drives that awaitable as a Rust `Future`, mapping results to the `Stream` API:
    ///   - On success, extracts the yielded Python object into `M` and returns
    ///     `Poll::Ready(Some(Ok(M)))`.
    ///   - On `StopAsyncIteration`, returns `Poll::Ready(None)` to signal end of stream.
    ///   - On any other Python exception, returns `Poll::Ready(Some(Err(PyErr)))`.
    ///
    /// State machine
    /// - `next_fut: Option<Future>` holds the in-flight future for the next element.
    /// - When `next_fut` is `None`, we create a new future by calling `__anext__()`
    ///   on the Python async iterator, then converting that awaitable into a Rust
    ///   future using `into_future_with_locals()` bound to the saved event loop.
    /// - When the future resolves (Ready), we immediately clear `next_fut` so that the
    ///   subsequent poll will create a new future for the next item (or end on stop).
    ///
    /// Event loop / threading
    /// - We do not run Python on the current thread's event loop. Instead, we keep an
    ///   owned reference to the target loop (`self.event_loop`) and use
    ///   `TaskLocals::new(loop)` so the `__anext__` awaitable runs on that loop.
    /// - All Python API interactions are performed via `Python::attach(|py| ...)`,
    ///   which safely acquires the GIL on the current thread when needed.
    ///
    /// Error and termination semantics
    /// - If `__anext__` raises `StopAsyncIteration`, we return `None` and the stream
    ///   is finished.
    /// - If `__anext__` raises any other exception, we surface it as `Some(Err(err))`.
    ///   Typically, an async generator that raises will be terminated by Python; the
    ///   next call to `__anext__` will then yield `StopAsyncIteration` and we return
    ///   `None` on the following poll.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // If we don't currently have an in-flight future, create one.
        if this.next_fut.is_none() {
            let event_loop = this.event_loop.clone();
            let fut = Python::attach(|py| {
                // Call __anext__ on the bound iterator to get an awaitable.
                let awaitable = this.aiter.bind(py).call_method0("__anext__")?;
                // Build TaskLocals tied to our target loop, and convert awaitable to a Rust Future.
                let locals = pyo3_async_runtimes::TaskLocals::new(event_loop.bind(py).clone());
                let f = pyo3_async_runtimes::into_future_with_locals(&locals, awaitable.clone())
                    .expect("failed to create future for __anext__");
                Ok::<_, PyErr>(f)
            })?;
            *this.next_fut = Some(Box::pin(fut));
        }

        // Poll the Python -> Rust future.
        match this.next_fut.as_mut().as_pin_mut() {
            None => Poll::Pending,
            Some(fut) => match fut.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(res) => {
                    // Consume and clear the in-flight future. This is crucial: it ensures
                    // the next call to `poll_next` will construct a fresh `__anext__()`
                    // awaitable for the subsequent element (or detect StopAsyncIteration).
                    // Without clearing this, we’d keep polling a completed future.
                    this.next_fut.set(None);
                    match res {
                        Ok(obj) => {
                            // Convert PyObject -> M
                            let m = Python::attach(|py| obj.extract::<M>(py));
                            Poll::Ready(Some(m))
                        }
                        Err(err) => {
                            // Stop on StopAsyncIteration; otherwise surface the error.
                            let is_stop =
                                Python::attach(|py| err.is_instance_of::<PyStopAsyncIteration>(py));
                            if is_stop {
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Err(err)))
                            }
                        }
                    }
                }
            },
        }
    }
}

/// Generic async iterator that yields items of type `T` from a Tokio mpsc channel.
/// This is the internal implementation used by module-specific PyAsyncDatumStream wrappers.
///
/// This struct is NOT exposed to Python directly. Instead, each module (reduce, session_reduce,
/// batchmap) wraps this in a `#[pyclass]` struct that delegates to this implementation.
///
/// Type `T` must implement `PyClass` so items can be converted to Python objects via `Py::new`.
pub struct AsyncChannelStream<T> {
    rx: Arc<AsyncMutex<mpsc::Receiver<T>>>,
}

impl<T> AsyncChannelStream<T>
where
    T: PyClass + Send + 'static,
    T: Into<PyClassInitializer<T>>,
{
    /// Create a new AsyncChannelStream from a Tokio mpsc receiver.
    pub fn new(rx: mpsc::Receiver<T>) -> Self {
        Self {
            rx: Arc::new(AsyncMutex::new(rx)),
        }
    }

    /// Implementation of Python's __aiter__ protocol.
    /// Returns a reference to self (the iterator itself).
    ///
    /// This should be called from the `#[pyclass]` wrapper's __aiter__ method.
    pub fn py_aiter<'a>(&self, slf: &'a Bound<'_, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        Ok(slf.clone())
    }

    /// Implementation of Python's __anext__ protocol.
    /// Returns a future that resolves to the next item from the channel.
    ///
    /// This should be called from the `#[pyclass]` wrapper's __anext__ method.
    pub fn py_anext<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let rx = self.rx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = rx.lock().await;
            match guard.recv().await {
                Some(item) => Python::attach(|py| {
                    let bound = Bound::new(py, item)?;
                    Ok(bound.into_any().unbind())
                }),
                None => Err(PyStopAsyncIteration::new_err("stream closed")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncChannelStream, PyAsyncIterStream};
    use pyo3::prelude::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;

    // Spawn a dedicated asyncio event loop running forever and return it.
    async fn spawn_event_loop() -> (tokio::task::JoinHandle<()>, Arc<Py<PyAny>>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let aio: Py<PyAny> = py.import("asyncio").unwrap().into();
                let event_loop = aio.call_method0(py, "new_event_loop").unwrap();
                let _ = tx.send(event_loop.clone_ref(py));
                event_loop.call_method0(py, "run_forever").unwrap();
            });
        });
        let loop_obj = rx.await.expect("event loop create");
        (handle, std::sync::Arc::new(loop_obj))
    }

    #[tokio::test]
    async fn py_async_iter_stream_yields_incrementally() {
        // Initialize the Python interpreter for use from Rust
        pyo3::Python::initialize();
        let (loop_handle, loop_obj) = spawn_event_loop().await;

        // Create a Python async generator that yields items with delays
        let agen_obj = Python::attach(|py| {
            use pyo3::types::PyDict;
            let code = r#"
import asyncio
from typing import AsyncIterator
async def agen() -> AsyncIterator[int]:
    for i in range(3):
        await asyncio.sleep(0.05)
        yield i
result = agen()
"#;
            let globals = PyDict::new(py);
            use std::ffi::CString;
            let code_c = CString::new(code).unwrap();
            py.run(&code_c, Some(&globals), None).unwrap();
            let any: Option<Bound<PyAny>> = globals.get_item("result").expect("result missing");
            let obj: Py<PyAny> = any.expect("result missing").unbind();
            obj
        });

        let mut stream =
            PyAsyncIterStream::<i64>::new(agen_obj, loop_obj.clone()).expect("construct stream");

        let t0 = tokio::time::Instant::now();
        let v1 = stream.next().await.unwrap().unwrap();
        let t1 = tokio::time::Instant::now();
        let v2 = stream.next().await.unwrap().unwrap();
        let t2 = tokio::time::Instant::now();
        let v3 = stream.next().await.unwrap().unwrap();
        let _t3 = tokio::time::Instant::now();
        let end = stream.next().await;

        assert_eq!(v1, 0);
        assert!(t1.duration_since(t0) >= tokio::time::Duration::from_millis(40));
        assert_eq!(v2, 1);
        assert!(t2.duration_since(t1) >= tokio::time::Duration::from_millis(40));
        assert_eq!(v3, 2);
        assert!(end.is_none());

        // Stop the event loop and wait for the loop thread to finish
        Python::attach(|py| {
            if let Ok(stop_cb) = loop_obj.getattr(py, "stop") {
                let _ = loop_obj.call_method1(py, "call_soon_threadsafe", (stop_cb,));
            }
        });
        let _ = loop_handle.await;
    }

    #[tokio::test]
    async fn py_async_iter_stream_yields_error() {
        // No delay; the async iterable should error on first next
        pyo3::Python::initialize();
        let (loop_handle, loop_obj) = spawn_event_loop().await;

        // Create a Python async generator that immediately raises
        let agen_obj = Python::attach(|py| {
            use pyo3::types::PyDict;
            let code = r#"
from typing import AsyncIterator
async def agen_fail() -> AsyncIterator[int]:
    if True:
        raise RuntimeError('boom')
    yield 0
result = agen_fail()
"#;
            let globals = PyDict::new(py);
            use std::ffi::CString;
            let code_c = CString::new(code).unwrap();
            py.run(&code_c, Some(&globals), None).unwrap();
            let any = globals.get_item("result").expect("result missing");
            any.expect("result missing").unbind()
        });

        let mut stream = PyAsyncIterStream::<i64>::new(agen_obj, loop_obj.clone()).unwrap();

        // First next should be an error
        let first = stream.next().await;
        assert!(first.is_some());
        let err = first
            .unwrap()
            .expect_err("expected error from async iterable");
        let is_runtime =
            Python::attach(|py| err.is_instance_of::<pyo3::exceptions::PyRuntimeError>(py));
        assert!(is_runtime, "error should be RuntimeError");

        // Subsequent next should terminate the stream (StopAsyncIteration)
        let end = stream.next().await;
        assert!(end.is_none());

        // Stop the event loop and wait for the loop thread to finish
        Python::attach(|py| {
            if let Ok(stop_cb) = loop_obj.getattr(py, "stop") {
                let _ = loop_obj.call_method1(py, "call_soon_threadsafe", (stop_cb,));
            }
        });
        let _ = loop_handle.await;
    }

    // Test data structure for AsyncChannelStream tests
    #[pyclass]
    #[derive(Clone)]
    struct TestDatum {
        #[pyo3(get)]
        value: i32,
        #[pyo3(get)]
        message: String,
    }

    #[pymethods]
    impl TestDatum {
        #[new]
        fn new(value: i32, message: String) -> Self {
            Self { value, message }
        }
    }

    #[tokio::test]
    async fn async_channel_stream_yields_items() {
        // Initialize Python interpreter
        pyo3::Python::initialize();

        // Create a channel and send some items
        let (tx, rx) = mpsc::channel::<TestDatum>(10);

        // Send test data
        tx.send(TestDatum::new(1, "first".to_string()))
            .await
            .unwrap();
        tx.send(TestDatum::new(2, "second".to_string()))
            .await
            .unwrap();
        tx.send(TestDatum::new(3, "third".to_string()))
            .await
            .unwrap();
        drop(tx); // Close the channel

        // Create AsyncChannelStream
        let stream = AsyncChannelStream::new(rx);

        // Test by calling from Python
        Python::attach(|py| {
            // Create a Python wrapper to test the stream
            let code = r#"
async def consume_stream(stream):
    results = []
    async for item in stream:
        results.append((item.value, item.message))
    return results
"#;
            use pyo3::types::PyDict;
            let globals = PyDict::new(py);
            use std::ffi::CString;
            let code_c = CString::new(code).unwrap();
            py.run(&code_c, Some(&globals), None).unwrap();

            let consume_fn = globals.get_item("consume_stream").unwrap().unwrap();

            // Create a simple wrapper class to expose the stream
            #[pyclass]
            struct StreamWrapper {
                inner: AsyncChannelStream<TestDatum>,
            }

            #[pymethods]
            impl StreamWrapper {
                fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
                    self.inner.py_anext(py)
                }
            }

            let wrapper = Py::new(py, StreamWrapper { inner: stream }).unwrap();

            // Run the async function
            let asyncio = py.import("asyncio").unwrap();
            let result = asyncio
                .call_method1("run", (consume_fn.call1((wrapper,)).unwrap(),))
                .unwrap();

            // Verify results
            let results: Vec<(i32, String)> = result.extract().unwrap();
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], (1, "first".to_string()));
            assert_eq!(results[1], (2, "second".to_string()));
            assert_eq!(results[2], (3, "third".to_string()));
        });
    }

    #[tokio::test]
    async fn async_channel_stream_handles_empty_channel() {
        pyo3::Python::initialize();

        // Create a channel and immediately close it
        let (tx, rx) = mpsc::channel::<TestDatum>(10);
        drop(tx);

        let stream = AsyncChannelStream::new(rx);

        Python::attach(|py| {
            let code = r#"
async def consume_empty_stream(stream):
    count = 0
    async for _ in stream:
        count += 1
    return count
"#;
            use pyo3::types::PyDict;
            let globals = PyDict::new(py);
            use std::ffi::CString;
            let code_c = CString::new(code).unwrap();
            py.run(&code_c, Some(&globals), None).unwrap();

            let consume_fn = globals.get_item("consume_empty_stream").unwrap().unwrap();

            #[pyclass]
            struct StreamWrapper {
                inner: AsyncChannelStream<TestDatum>,
            }

            #[pymethods]
            impl StreamWrapper {
                fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
                    self.inner.py_anext(py)
                }
            }

            let wrapper = Py::new(py, StreamWrapper { inner: stream }).unwrap();

            let asyncio = py.import("asyncio").unwrap();
            let result = asyncio
                .call_method1("run", (consume_fn.call1((wrapper,)).unwrap(),))
                .unwrap();

            let count: i32 = result.extract().unwrap();
            assert_eq!(count, 0, "Empty stream should yield no items");
        });
    }

    #[tokio::test]
    async fn async_channel_stream_yields_multiple_items() {
        pyo3::Python::initialize();

        let (tx, rx) = mpsc::channel::<TestDatum>(10);

        // Send multiple items
        for i in 1..=5 {
            tx.send(TestDatum::new(i, format!("item_{}", i)))
                .await
                .unwrap();
        }
        drop(tx); // Close the channel

        let stream = AsyncChannelStream::new(rx);

        Python::attach(|py| {
            let code = r#"
async def consume_all(stream):
    results = []
    async for item in stream:
        results.append((item.value, item.message))
    return results
"#;
            use pyo3::types::PyDict;
            let globals = PyDict::new(py);
            use std::ffi::CString;
            let code_c = CString::new(code).unwrap();
            py.run(&code_c, Some(&globals), None).unwrap();

            let consume_fn = globals.get_item("consume_all").unwrap().unwrap();

            #[pyclass]
            struct StreamWrapper {
                inner: AsyncChannelStream<TestDatum>,
            }

            #[pymethods]
            impl StreamWrapper {
                fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                    slf
                }

                fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
                    self.inner.py_anext(py)
                }
            }

            let wrapper = Py::new(py, StreamWrapper { inner: stream }).unwrap();

            let asyncio = py.import("asyncio").unwrap();
            let result = asyncio
                .call_method1("run", (consume_fn.call1((wrapper,)).unwrap(),))
                .unwrap();

            let results: Vec<(i32, String)> = result.extract().unwrap();
            assert_eq!(results.len(), 5);
            for i in 1..=5 {
                assert_eq!(results[i - 1], (i as i32, format!("item_{}", i)));
            }
        });
    }

    #[test]
    fn async_channel_stream_production_workload() {
        // This test simulates production workload where:
        // - Channel buffer size is 1 (backpressure)
        // - Data is produced incrementally by a Tokio task
        // - Data is streamed to Python as it arrives (not buffered)
        pyo3::Python::initialize();

        // Create a Tokio runtime for this test
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // Buffer size of 1 to simulate real backpressure
            let (tx, rx) = mpsc::channel::<TestDatum>(1);

            // Spawn a producer task that sends items with small delays
            // This simulates data arriving from an external source (e.g., gRPC stream)
            let producer = tokio::spawn(async move {
                for i in 1..=10 {
                    // Simulate some processing/network delay
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

                    // This will block if the channel is full (backpressure)
                    // With buffer size 1, this ensures items are sent one at a time
                    if tx
                        .send(TestDatum::new(i, format!("datum_{}", i)))
                        .await
                        .is_err()
                    {
                        break; // Receiver dropped
                    }
                }
                // Close the channel after sending all items
            });

            let stream = AsyncChannelStream::new(rx);

            // Test by manually calling __anext__ multiple times
            // This simulates how Python would consume the stream
            let results = Python::attach(|py| {
                #[pyclass]
                struct StreamWrapper {
                    inner: AsyncChannelStream<TestDatum>,
                }

                #[pymethods]
                impl StreamWrapper {
                    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                        slf
                    }

                    fn __anext__<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
                        self.inner.py_anext(py)
                    }
                }

                let wrapper = Py::new(py, StreamWrapper { inner: stream }).unwrap();

                // Manually consume items by calling __anext__ in a loop
                // This is what Python's "async for" does under the hood
                let code = r#"
async def consume_stream(stream):
    results = []
    try:
        while True:
            item = await stream.__anext__()
            results.append((item.value, item.message))
    except StopAsyncIteration:
        pass
    return results
"#;
                use pyo3::types::PyDict;
                let globals = PyDict::new(py);
                use std::ffi::CString;
                let code_c = CString::new(code).unwrap();
                py.run(&code_c, Some(&globals), None).unwrap();

                let consume_fn = globals.get_item("consume_stream").unwrap().unwrap();

                // Use asyncio.run to execute the async function
                let asyncio = py.import("asyncio").unwrap();
                let result = asyncio
                    .call_method1("run", (consume_fn.call1((wrapper,)).unwrap(),))
                    .unwrap();

                result.extract::<Vec<(i32, String)>>().unwrap()
            });

            // Wait for producer to complete
            producer.await.unwrap();

            // Verify all items were received in order
            assert_eq!(results.len(), 10, "Should receive all 10 items");
            for i in 1..=10 {
                assert_eq!(
                    results[i - 1],
                    (i as i32, format!("datum_{}", i)),
                    "Item {} should match",
                    i
                );
            }
        });
    }
}

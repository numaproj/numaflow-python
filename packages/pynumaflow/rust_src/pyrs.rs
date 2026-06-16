use pyo3::{Py, PyAny, PyErr, Python};
use std::sync::Arc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;

/// Start a dedicated asyncio event loop on this (blocking) thread and run it
/// forever. The created loop is sent back to the caller via `tx` so that gRPC
/// request handlers can schedule the user's coroutines onto it.
///
/// Creating the loop can fail (e.g. asyncio import errors). Rather than
/// panicking on this dedicated thread - which would only surface to the caller
/// as an opaque `RecvError` once `tx` is dropped - we send the `PyErr` back so
/// the caller can propagate a clean error.
pub(crate) fn run_asyncio(tx: Sender<Result<Arc<Py<PyAny>>, PyErr>>) {
    let event_loop = Python::attach(|py| -> Result<Arc<Py<PyAny>>, PyErr> {
        let aio: Py<PyAny> = py.import("asyncio")?.into();
        let event_loop = aio.call_method0(py, "new_event_loop")?;
        Ok(Arc::new(event_loop))
    });

    let event_loop = match event_loop {
        Ok(event_loop) => event_loop,
        Err(err) => {
            // Report the failure to the caller and stop; there is no loop to run.
            let _ = tx.send(Err(err));
            return;
        }
    };

    let _ = tx.send(Ok(event_loop.clone()));

    Python::attach(|py| {
        // `run_forever` only returns once the loop is stopped (see `start` in
        // server.rs, which calls `loop.stop()` on shutdown). A failure here is
        // unrecoverable on this dedicated thread, so panic with a descriptive
        // message rather than an opaque `.unwrap()`. `.expect` appends the
        // `PyErr` to the message for us.
        event_loop
            .call_method0(py, "run_forever")
            .expect("asyncio event loop terminated with an error");
    });
}

pub(crate) fn setup_sig_handler(shutdown_rx: Receiver<()>) -> (JoinHandle<()>, Receiver<()>) {
    // Listen for OS signals (Ctrl+C and SIGTERM) to trigger shutdown from Rust as well.
    let (os_sig_tx, mut os_sig_rx) = tokio::sync::oneshot::channel::<()>();

    let sig_handle = tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        let mut sigterm_stream =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        #[cfg(unix)]
        let sigterm = sigterm_stream.recv();
        #[cfg(not(unix))]
        let sigterm = std::future::pending::<()>();
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm => {},
        }
        let _ = os_sig_tx.send(());
    });

    // Combine Python-initiated shutdown and OS signal shutdown into one channel for the server.
    let (combined_tx, combined_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_rx => {},
            _ = &mut os_sig_rx => {},
        }
        let _ = combined_tx.send(());
    });

    (sig_handle, combined_rx)
}

use pyo3::{Py, PyAny, Python};
use std::sync::Arc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;

/// Start asyncio event loop and block on it forever
pub(crate) fn run_asyncio(tx: Sender<Arc<Py<PyAny>>>) {
    let event_loop: Py<PyAny> = Python::attach(|py| {
        let aio: Py<PyAny> = py.import("asyncio").unwrap().into();
        aio.call_method0(py, "new_event_loop").unwrap()
    });
    let event_loop = Arc::new(event_loop);
    let _ = tx.send(event_loop.clone());
    Python::attach(|py| {
        println!("Starting NumaflowCore: event_loop={:?}", event_loop);
        event_loop.call_method0(py, "run_forever").unwrap();
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

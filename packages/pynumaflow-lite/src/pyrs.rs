use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::{Py, PyAny, PyErr, Python};
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

// Build the full Python traceback text for the panic message, so the sidecar
// reports the same failure that Python raises.
pub(crate) fn format_error(py: Python<'_>, error: &PyErr) -> String {
    match error.traceback(py).map(|traceback| traceback.format()) {
        Some(Ok(traceback)) => format!("{traceback}{error}"),
        _ => error.to_string(),
    }
}

// Join every handler failure into one error for Python to raise.
//
// Python 3.11 and later have BaseExceptionGroup, which prints each traceback in
// turn. Older versions have no group type, so they get the first error only.
pub(crate) fn combine_errors(py: Python<'_>, errors: Vec<PyErr>) -> PyErr {
    let first = || errors.first().expect("errors is never empty").clone_ref(py);

    if errors.len() == 1 {
        return first();
    }

    let Ok(group_type) = py
        .import("builtins")
        .and_then(|builtins| builtins.getattr("BaseExceptionGroup"))
    else {
        return first();
    };

    let values: Vec<_> = errors.iter().map(|error| error.value(py).clone()).collect();
    let message = format!("{} map handler calls failed", values.len());

    match group_type.call1((message, values)) {
        Ok(group) => PyErr::from_value(group),
        Err(_) => first(),
    }
}

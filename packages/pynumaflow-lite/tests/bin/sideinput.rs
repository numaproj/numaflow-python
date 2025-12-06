use std::env;
use std::path::PathBuf;

use numaflow::proto::side_input::side_input_client::SideInputClient;
use tokio::net::UnixStream;
use tonic::transport::Uri;
use tower::service_fn;

// Simple Rust client binary that exercises the SideInput server over Unix Domain Socket.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_SIDEINPUT_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/sideinput.sock".to_string());

    // Set up tonic channel over Unix Domain Socket.
    let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            let sock = PathBuf::from(sock_path.clone());
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(sock).await?,
                ))
            }
        }))
        .await?;

    let mut client = SideInputClient::new(channel);

    // Test 1: First call should broadcast (counter = 1, odd)
    let response = client.retrieve_side_input(()).await?;
    let resp = response.into_inner();
    println!("Response 1: value={:?}, no_broadcast={}", String::from_utf8_lossy(&resp.value), resp.no_broadcast);
    assert!(!resp.no_broadcast, "First call should broadcast");
    assert!(!resp.value.is_empty(), "First call should have a value");
    assert!(
        String::from_utf8_lossy(&resp.value).starts_with("an example:"),
        "Value should start with 'an example:'"
    );

    // Test 2: Second call should NOT broadcast (counter = 2, even)
    let response = client.retrieve_side_input(()).await?;
    let resp = response.into_inner();
    println!("Response 2: value={:?}, no_broadcast={}", String::from_utf8_lossy(&resp.value), resp.no_broadcast);
    assert!(resp.no_broadcast, "Second call should not broadcast");
    assert!(resp.value.is_empty(), "Second call should have empty value");

    // Test 3: Third call should broadcast again (counter = 3, odd)
    let response = client.retrieve_side_input(()).await?;
    let resp = response.into_inner();
    println!("Response 3: value={:?}, no_broadcast={}", String::from_utf8_lossy(&resp.value), resp.no_broadcast);
    assert!(!resp.no_broadcast, "Third call should broadcast");
    assert!(!resp.value.is_empty(), "Third call should have a value");

    // Test 4: Fourth call should NOT broadcast (counter = 4, even)
    let response = client.retrieve_side_input(()).await?;
    let resp = response.into_inner();
    println!("Response 4: value={:?}, no_broadcast={}", String::from_utf8_lossy(&resp.value), resp.no_broadcast);
    assert!(resp.no_broadcast, "Fourth call should not broadcast");

    // Test is_ready endpoint
    let ready_response = client.is_ready(()).await?;
    let ready = ready_response.into_inner();
    println!("IsReady: {}", ready.ready);
    assert!(ready.ready, "Server should be ready");

    println!("All side input tests passed!");

    Ok(())
}


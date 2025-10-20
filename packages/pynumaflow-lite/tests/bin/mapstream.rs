use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::map::map_client::MapClient;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Uri;
use tower::service_fn;

// Rust client binary to exercise the MapStream server (streaming outputs).
// It connects over Unix Domain Socket to the mapstream server, sends a single
// request with comma-separated values, and asserts it receives multiple
// MapResponse messages, each containing one split as the value.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_MAPSTREAM_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/mapstream.sock".to_string());

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

    let mut client = MapClient::new(channel);

    // Build one request with comma-separated payload
    let request = proto::map::MapRequest {
        request: Some(proto::map::map_request::Request {
            keys: vec!["k".into()],
            value: "a,b,c".as_bytes().to_vec(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        }),
        id: "".to_string(),
        handshake: None,
        status: None,
    };

    // Create request stream with handshake first
    let (tx, rx) = mpsc::channel(8);
    let handshake_request = proto::map::MapRequest {
        request: None,
        id: "".to_string(),
        handshake: Some(proto::map::Handshake { sot: true }),
        status: None,
    };
    tx.send(handshake_request).await.unwrap();

    // Start the RPC
    let response_stream = client.map_fn(ReceiverStream::new(rx)).await.unwrap();
    let mut response_stream = response_stream.into_inner();

    // After handshake, send the real request
    let tx_ref = tx;
    tx_ref.send(request).await.unwrap();

    // Expect three MapResponse messages, each carrying one result value
    let mut got: Vec<Vec<u8>> = Vec::new();
    while got.len() < 3 {
        let maybe = response_stream.message().await.unwrap();
        assert!(maybe.is_some());
        let resp = maybe.unwrap();
        // Each MapResponse carries results; we take the first
        if let Some(first) = resp.results.get(0) {
            got.push(first.value.clone());
        }
    }

    assert_eq!(got[0], b"a".to_vec());
    assert_eq!(got[1], b"b".to_vec());
    assert_eq!(got[2], b"c".to_vec());

    // Close request stream
    drop(tx_ref);

    Ok(())
}

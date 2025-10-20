use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::map::map_client::MapClient;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Uri;
use tower::service_fn;

// Simple Rust client binary that exercises the Map server over Unix Domain Socket.
// This test is a complex one. To start it, first manually run the python code and then
// run this test.
// You can run the python code with:
//    maturin develop && python simple_cat.py
// This won't exit, so kill it after running the tests
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_MAP_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/map.sock".to_string());

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

    let (tx, rx) = mpsc::channel(8);

    // Handshake
    let handshake_request = proto::map::MapRequest {
        request: None,
        id: "".to_string(),
        handshake: Some(proto::map::Handshake { sot: true }),
        status: None,
    };
    tx.send(handshake_request).await.unwrap();

    let resp = client.map_fn(ReceiverStream::new(rx)).await.unwrap();
    let mut resp = resp.into_inner();

    let handshake_response = resp.message().await.unwrap();
    assert!(handshake_response.is_some());
    let handshake_response = handshake_response.unwrap();
    assert!(handshake_response.handshake.is_some());

    // Request 1
    let request_1 = proto::map::MapRequest {
        request: Some(proto::map::map_request::Request {
            keys: vec!["first".into(), "second".into()],
            value: "hello".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        }),
        id: "".to_string(),
        handshake: None,
        status: None,
    };
    // use a fresh channel tx for requests after map_fn is created
    // but tonic's client holds the stream created above; we keep using tx
    // created pre-call. So just continue sending on tx via clone.
    // (We still hold `tx` by move; creating clone in case of future change.)

    // Request stream sender recovered via a channel before map_fn invocation.
    // The tx is still in scope here; send requests.
    // Note: if you change channel creation, ensure tx lives long enough.

    // We must keep `tx` alive until all messages are sent.
    let tx_ref = tx;
    tx_ref.send(request_1).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let r = actual_response.unwrap();
    let msg = &r.results[0];
    assert_eq!(msg.keys.first(), Some(&"first".to_owned()));
    assert_eq!(msg.value, "hello".as_bytes());

    // Request 2
    let request_2 = proto::map::MapRequest {
        request: Some(proto::map::map_request::Request {
            keys: vec!["third".into(), "fourth".into()],
            value: "world".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        }),
        id: "".to_string(),
        handshake: None,
        status: None,
    };
    tx_ref.send(request_2).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let msg = &actual_response.unwrap().results[0];
    assert_eq!(msg.keys.first(), Some(&"third".to_owned()));
    assert_eq!(msg.value, "world".as_bytes());

    // Request 3 (drop)
    let request_3 = proto::map::MapRequest {
        request: Some(proto::map::map_request::Request {
            keys: vec!["third".into(), "fourth".into()],
            value: "bad world".into(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
        }),
        id: "".to_string(),
        handshake: None,
        status: None,
    };
    tx_ref.send(request_3).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let msg = &actual_response.unwrap().results[0];
    assert_eq!(msg.tags, vec![numaflow::shared::DROP.to_string()]);

    // close request stream
    drop(tx_ref);

    Ok(())
}

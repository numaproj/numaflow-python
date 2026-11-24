use std::collections::HashMap;
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

    // Build one request with comma-separated payload, with user and system metadata
    let request = proto::map::MapRequest {
        request: Some(proto::map::map_request::Request {
            keys: vec!["k".into()],
            value: "a,b,c".as_bytes().to_vec(),
            watermark: Some(prost_types::Timestamp::default()),
            event_time: Some(prost_types::Timestamp::default()),
            headers: Default::default(),
            metadata: Some(proto::metadata::Metadata {
                previous_vertex: "in".to_string(),
                sys_metadata: HashMap::from([(
                    "sys_group".to_string(),
                    kv_group(&[("sys_key", b"sys_value")]),
                )]),
                user_metadata: HashMap::from([(
                    "user_group".to_string(),
                    kv_group(&[("user_key", b"user_value")]),
                )]),
            }),
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

    // Expect three MapResponse messages, each carrying one result
    let mut got: Vec<proto::map::map_response::Result> = Vec::new();
    while got.len() < 3 {
        let maybe = response_stream.message().await.unwrap();
        assert!(maybe.is_some());
        let resp = maybe.unwrap();
        // Each MapResponse carries results; we take the first
        if let Some(first) = resp.results.into_iter().next() {
            got.push(first);
        }
    }

    assert_eq!(got[0].value, b"a".to_vec());
    assert_eq!(got[1].value, b"b".to_vec());
    assert_eq!(got[2].value, b"c".to_vec());

    for result in &got {
        let metadata = result.metadata.as_ref().expect("metadata is set");
        // Verify the handler received the incoming user metadata in Datum.user_metadata
        assert_eq!(
            metadata.user_metadata["user_group"].key_value["user_key"],
            b"user_value".to_vec()
        );
        // Verify the handler received the incoming system metadata in Datum.system_metadata
        assert_eq!(
            metadata.user_metadata["system_echo"].key_value["sys_group.sys_key"],
            b"sys_value".to_vec()
        );
        // System metadata is never sent back by the UDF
        assert!(metadata.sys_metadata.is_empty());
    }

    // Close request stream
    drop(tx_ref);

    Ok(())
}

fn kv_group(pairs: &[(&str, &[u8])]) -> proto::metadata::KeyValueGroup {
    proto::metadata::KeyValueGroup {
        key_value: pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_vec()))
            .collect(),
    }
}

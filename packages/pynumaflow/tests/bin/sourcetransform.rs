use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::metadata::{KeyValueGroup, Metadata};
use numaflow::proto::source_transformer::source_transform_client::SourceTransformClient;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Uri;
use tower::service_fn;

// Simple Rust client binary that exercises the SourceTransform server over a Unix Domain Socket.
//
// The Python `grpcio` client cannot interoperate with the tonic gRPC server over a UDS, so we
// drive the server from a tonic client here, mirroring the pynumaflow-lite harness.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_SOURCETRANSFORM_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/sourcetransform.sock".to_string());

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

    let mut client = SourceTransformClient::new(channel);

    let (tx, rx) = mpsc::channel(8);

    // Handshake
    let handshake_request = proto::source_transformer::SourceTransformRequest {
        request: None,
        handshake: Some(proto::source_transformer::Handshake { sot: true }),
    };
    tx.send(handshake_request).await.unwrap();

    let resp = client
        .source_transform_fn(ReceiverStream::new(rx))
        .await
        .unwrap();
    let mut resp = resp.into_inner();

    let handshake_response = resp.message().await.unwrap();
    assert!(handshake_response.is_some());
    let handshake_response = handshake_response.unwrap();
    assert!(handshake_response.handshake.is_some());

    // Request 1 - normal message (event time in 2023) -> tagged "after_year_2022".
    // Carries both system metadata (read-only; must NOT come back) and user
    // metadata (the handler passes it through; must round-trip).
    let request_metadata = Metadata {
        previous_vertex: "test-source".to_string(),
        sys_metadata: HashMap::from([(
            "numaflow_version_info".to_string(),
            KeyValueGroup {
                key_value: HashMap::from([("version".to_string(), b"1.0.0".to_vec())]),
            },
        )]),
        user_metadata: HashMap::from([(
            "filter_info".to_string(),
            KeyValueGroup {
                key_value: HashMap::from([(
                    "filter_result".to_string(),
                    b"after_year_2022".to_vec(),
                )]),
            },
        )]),
    };
    let request_1 = proto::source_transformer::SourceTransformRequest {
        request: Some(
            proto::source_transformer::source_transform_request::Request {
                id: "1".to_string(),
                keys: vec!["first".into(), "second".into()],
                value: "hello".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp {
                    seconds: 1672531200, // 2023-01-01 00:00:00 UTC
                    nanos: 0,
                }),
                headers: Default::default(),
                metadata: Some(request_metadata),
            },
        ),
        handshake: None,
    };
    tx.send(request_1).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let r = actual_response.unwrap();
    assert_eq!(r.id, "1");
    let msg = &r.results[0];
    assert_eq!(msg.keys.first(), Some(&"first".to_owned()));
    assert_eq!(msg.value, "hello".as_bytes());
    assert!(msg.tags.contains(&"after_year_2022".to_string()));
    // Verify event_time is set (re-stamped to Jan 1 2023).
    assert!(msg.event_time.is_some());

    // Verify metadata round-trip: the user metadata the handler passed through
    // comes back, while system metadata is empty in the response (users cannot
    // set it).
    let resp_metadata = msg
        .metadata
        .as_ref()
        .expect("response result should carry metadata");
    assert!(
        resp_metadata.sys_metadata.is_empty(),
        "system metadata must be empty in the response, got {:?}",
        resp_metadata.sys_metadata
    );
    let user_group = resp_metadata
        .user_metadata
        .get("filter_info")
        .expect("user metadata group 'filter_info' should be present");
    assert_eq!(
        user_group.key_value.get("filter_result"),
        Some(&b"after_year_2022".to_vec()),
        "user metadata should round-trip unchanged"
    );

    // Request 2 - message to be dropped (event time in 2021)
    let request_2 = proto::source_transformer::SourceTransformRequest {
        request: Some(
            proto::source_transformer::source_transform_request::Request {
                id: "2".to_string(),
                keys: vec!["third".into(), "fourth".into()],
                value: "old_message".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp {
                    seconds: 1609459200, // 2021-01-01 00:00:00 UTC
                    nanos: 0,
                }),
                headers: Default::default(),
                metadata: None,
            },
        ),
        handshake: None,
    };
    tx.send(request_2).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let r = actual_response.unwrap();
    assert_eq!(r.id, "2");
    let msg = &r.results[0];
    assert_eq!(msg.tags, vec![numaflow::shared::DROP.to_string()]);

    // Request 3 - message within 2022 -> tagged "within_year_2022"
    let request_3 = proto::source_transformer::SourceTransformRequest {
        request: Some(
            proto::source_transformer::source_transform_request::Request {
                id: "3".to_string(),
                keys: vec!["fifth".into()],
                value: "year_2022_message".into(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp {
                    seconds: 1656633600, // 2022-07-01 00:00:00 UTC
                    nanos: 0,
                }),
                headers: Default::default(),
                metadata: None,
            },
        ),
        handshake: None,
    };
    tx.send(request_3).await.unwrap();

    let actual_response = resp.message().await.unwrap();
    assert!(actual_response.is_some());
    let r = actual_response.unwrap();
    assert_eq!(r.id, "3");
    let msg = &r.results[0];
    assert_eq!(msg.value, "year_2022_message".as_bytes());
    assert!(msg.tags.contains(&"within_year_2022".to_string()));

    // close request stream
    drop(tx);

    Ok(())
}

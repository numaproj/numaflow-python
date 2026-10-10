use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::source_transformer::source_transform_client::SourceTransformClient;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Uri;
use tower::service_fn;

// Simple Rust client binary that exercises the SourceTransform server over Unix Domain Socket.
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

    // Request 1 - normal message (event time in 2023), with user and system metadata
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
    // Verify the handler set the event time to 2023-01-01 00:00:00 UTC
    assert_eq!(
        msg.event_time,
        Some(prost_types::Timestamp {
            seconds: 1672531200,
            nanos: 0,
        })
    );
    assert!(msg.tags.contains(&"after_year_2022".to_string()));
    let user_metadata = &msg
        .metadata
        .as_ref()
        .expect("metadata is set")
        .user_metadata;
    // Verify the user metadata that the handler created
    assert_eq!(
        user_metadata["filter_info"].key_value["filter_result"],
        b"after_year_2022".to_vec()
    );
    // Verify the handler received the incoming user metadata in Datum.user_metadata
    assert_eq!(
        user_metadata["user_group"].key_value["user_key"],
        b"user_value".to_vec()
    );
    // Verify the handler received the incoming system metadata in Datum.system_metadata
    assert_eq!(
        user_metadata["system_echo"].key_value["sys_group.sys_key"],
        b"sys_value".to_vec()
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

    // Request 3 - message within 2022
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
    // Verify the handler set the event time to 2022-01-01 00:00:00 UTC
    assert_eq!(
        msg.event_time,
        Some(prost_types::Timestamp {
            seconds: 1640995200,
            nanos: 0,
        })
    );
    assert!(msg.tags.contains(&"within_year_2022".to_string()));
    // Verify the user metadata that the handler created
    let user_metadata = &msg
        .metadata
        .as_ref()
        .expect("metadata is set")
        .user_metadata;
    assert_eq!(
        user_metadata["filter_info"].key_value["filter_result"],
        b"within_year_2022".to_vec()
    );
    // No metadata was sent, so the handler forwards no user groups and echoes no system metadata
    assert!(!user_metadata.contains_key("user_group"));
    assert!(
        user_metadata
            .get("system_echo")
            .is_none_or(|g| g.key_value.is_empty())
    );

    // close request stream
    drop(tx);

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

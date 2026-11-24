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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_BATCHMAP_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/batchmap.sock".to_string());

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

    let (tx, rx) = mpsc::channel(16);

    // Handshake to initialize the stream
    let handshake_request = proto::map::MapRequest {
        request: None,
        id: "".to_string(),
        handshake: Some(proto::map::Handshake { sot: true }),
        status: None,
    };
    tx.send(handshake_request).await.unwrap();

    let resp = client.map_fn(ReceiverStream::new(rx)).await.unwrap();
    let mut resp = resp.into_inner();

    // Expect handshake response from server
    let handshake_response = resp.message().await.unwrap();
    assert!(handshake_response.is_some());
    let handshake_response = handshake_response.unwrap();
    assert!(handshake_response.handshake.is_some());

    // Build three requests with IDs
    let mk_req = |id: &str,
                  keys: Vec<&str>,
                  value: &str,
                  metadata: Option<proto::metadata::Metadata>|
     -> proto::map::MapRequest {
        proto::map::MapRequest {
            request: Some(proto::map::map_request::Request {
                keys: keys.into_iter().map(|s| s.to_string()).collect(),
                value: value.as_bytes().to_vec(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                headers: HashMap::new(),
                metadata,
            }),
            id: id.to_string(),
            handshake: None,
            status: None,
        }
    };

    // Request 1 carries user and system metadata; the others carry none
    let req1 = mk_req(
        "id-1",
        vec!["k1"],
        "hello-1",
        Some(proto::metadata::Metadata {
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
    );
    let req2 = mk_req("id-2", vec!["k2"], "hello-2", None);
    let req3 = mk_req("id-3", vec!["k3"], "hello-3", None);

    // Sender must live long enough; keep a clone for clarity
    let tx_ref = tx;
    tx_ref.send(req1).await.unwrap();
    tx_ref.send(req2).await.unwrap();
    tx_ref.send(req3).await.unwrap();

    // Send End-Of-Batch marker via status
    // This uses the proto Status message to indicate an EOT for the batch.
    let eot = numaflow::proto::map::TransmissionStatus { eot: true };
    let eot_req = proto::map::MapRequest {
        request: None,
        id: "eot".to_string(),
        handshake: None,
        status: Some(eot),
    };
    tx_ref.send(eot_req).await.unwrap();

    // Collect exactly 3 responses, one per id
    use std::collections::BTreeMap;
    let mut got: BTreeMap<String, proto::map::map_response::Result> = BTreeMap::new();

    while got.len() < 3 {
        let maybe = resp.message().await.unwrap();
        assert!(maybe.is_some());
        let r = maybe.unwrap();
        // MapResponse is expected to include an id and results list.
        // We'll take the first result.
        let result = r.results.into_iter().next().expect("result is set");
        got.insert(r.id, result);
    }

    assert_eq!(got["id-1"].value, "hello-1".as_bytes());
    assert_eq!(got["id-2"].value, "hello-2".as_bytes());
    assert_eq!(got["id-3"].value, "hello-3".as_bytes());

    let metadata = got["id-1"].metadata.as_ref().expect("metadata is set");
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

    // No metadata was sent, so the handler forwards no user groups and echoes no system metadata
    let user_metadata = &got["id-2"]
        .metadata
        .as_ref()
        .expect("metadata is set")
        .user_metadata;
    assert!(!user_metadata.contains_key("user_group"));
    assert!(
        user_metadata
            .get("system_echo")
            .is_none_or(|g| g.key_value.is_empty())
    );

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

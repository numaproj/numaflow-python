use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::sink::sink_client::SinkClient;
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
        .or_else(|| env::var("NUMAFLOW_SINK_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/sink.sock".to_string());

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

    let mut client = SinkClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    // Handshake to initialize the stream
    let handshake_request = proto::sink::SinkRequest {
        request: None,
        handshake: Some(proto::sink::Handshake { sot: true }),
        status: None,
    };
    tx.send(handshake_request).await.unwrap();

    let resp = client.sink_fn(ReceiverStream::new(rx)).await.unwrap();
    let mut resp = resp.into_inner();

    // Expect handshake response from server
    let handshake_response = resp.message().await.unwrap();
    assert!(handshake_response.is_some());
    let handshake_response = handshake_response.unwrap();
    assert!(handshake_response.handshake.is_some());

    // Build three requests with IDs
    let mk_req = |id: &str, keys: Vec<&str>, value: &str| -> proto::sink::SinkRequest {
        proto::sink::SinkRequest {
            request: Some(proto::sink::sink_request::Request {
                keys: keys.into_iter().map(|s| s.to_string()).collect(),
                value: value.as_bytes().to_vec(),
                watermark: Some(prost_types::Timestamp::default()),
                event_time: Some(prost_types::Timestamp::default()),
                id: id.to_string(),
                headers: HashMap::new(),
                metadata: None,
            }),
            handshake: None,
            status: None,
        }
    };

    let req1 = mk_req("id-1", vec!["k1"], "hello-1");
    let req2 = mk_req("id-2", vec!["k2"], "hello-2");
    let req3 = mk_req("id-3", vec!["k3"], "hello-3");

    // Sender must live long enough; keep a clone for clarity
    let tx_ref = tx;
    tx_ref.send(req1).await.unwrap();
    tx_ref.send(req2).await.unwrap();
    tx_ref.send(req3).await.unwrap();

    // Send End-Of-Transmission marker via status
    let eot = numaflow::proto::sink::TransmissionStatus { eot: true };
    let eot_req = proto::sink::SinkRequest {
        request: None,
        handshake: None,
        status: Some(eot),
    };
    tx_ref.send(eot_req).await.unwrap();

    // Collect the batch response
    let maybe = resp.message().await.unwrap();
    assert!(maybe.is_some());
    let r = maybe.unwrap();

    // Verify we got 3 responses
    assert_eq!(r.results.len(), 3);

    // Verify all responses are successful
    for result in &r.results {
        assert_eq!(result.status, proto::sink::Status::Success as i32);
        assert_eq!(result.err_msg, "");
    }

    // Verify the IDs match
    let ids: Vec<String> = r.results.iter().map(|res| res.id.clone()).collect();
    assert!(ids.contains(&"id-1".to_string()));
    assert!(ids.contains(&"id-2".to_string()));
    assert!(ids.contains(&"id-3".to_string()));

    // Expect EOT response
    let eot_response = resp.message().await.unwrap();
    assert!(eot_response.is_some());
    let eot_response = eot_response.unwrap();
    assert!(eot_response.status.is_some());
    assert!(eot_response.status.unwrap().eot);

    println!("All sink tests passed!");

    Ok(())
}

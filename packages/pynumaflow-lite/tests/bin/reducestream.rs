use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, transport::Uri};
use tower::service_fn;

fn now_ts() -> prost_types::Timestamp {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default socket path with env/arg override
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_REDUCESTREAM_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/reducestream.sock".to_string());

    // Connect over UDS
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

    // Reduce client from generated proto (reducestream uses the same proto as reduce)
    let mut client = numaflow::proto::reduce::reduce_client::ReduceClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    // Send 3 ReduceRequests to test streaming behavior
    // The Python handler should yield 3 messages (one per datum)
    {
        use numaflow::proto::reduce as reduce_proto;

        // First request with OPEN operation
        let rr1 = reduce_proto::ReduceRequest {
            payload: Some(reduce_proto::reduce_request::Payload {
                keys: vec!["test-key".into()],
                value: b"1".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(reduce_proto::reduce_request::WindowOperation {
                event: 0, // OPEN
                windows: vec![reduce_proto::Window {
                    start: Some(now_ts()),
                    end: Some(now_ts()),
                    slot: "slot-0".to_string(),
                }],
            }),
        };
        tx.send(rr1).await.unwrap();

        // Second request with APPEND operation
        let rr2 = reduce_proto::ReduceRequest {
            payload: Some(reduce_proto::reduce_request::Payload {
                keys: vec!["test-key".into()],
                value: b"2".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(reduce_proto::reduce_request::WindowOperation {
                event: 4, // APPEND
                windows: vec![reduce_proto::Window {
                    start: Some(now_ts()),
                    end: Some(now_ts()),
                    slot: "slot-0".to_string(),
                }],
            }),
        };
        tx.send(rr2).await.unwrap();

        // Third request with APPEND operation
        let rr3 = reduce_proto::ReduceRequest {
            payload: Some(reduce_proto::reduce_request::Payload {
                keys: vec!["test-key".into()],
                value: b"3".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(reduce_proto::reduce_request::WindowOperation {
                event: 4, // APPEND
                windows: vec![reduce_proto::Window {
                    start: Some(now_ts()),
                    end: Some(now_ts()),
                    slot: "slot-0".to_string(),
                }],
            }),
        };
        tx.send(rr3).await.unwrap();
    }

    // Drop sender to signal end-of-stream (COB)
    drop(tx);

    // Start the RPC using a Request-wrapped stream
    let request = Request::new(ReceiverStream::new(rx));
    let mut resp = client.reduce_fn(request).await?.into_inner();

    // Read responses - we should get 3 messages (one per datum) due to streaming
    let mut message_count = 0;
    let mut found_eof = false;

    loop {
        if let Some(r) = resp.message().await? {
            if let Some(res) = r.result {
                assert!(!res.value.is_empty());
                message_count += 1;
                println!(
                    "Received message {}: {:?}",
                    message_count,
                    String::from_utf8_lossy(&res.value)
                );
            }
            if r.eof {
                found_eof = true;
                println!("Received EOF");
                break;
            }
        } else {
            break;
        }
    }

    // Verify we got 3 messages (streaming behavior)
    assert_eq!(
        message_count, 3,
        "Expected 3 streamed messages, got {}",
        message_count
    );
    assert!(found_eof, "Should have received EOF");

    println!("Test passed! Received {} streamed messages", message_count);

    Ok(())
}

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

fn ts_from_secs(secs: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: secs,
        nanos: 0,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Default socket path with env/arg override
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_ACCUMULATOR_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/accumulator.sock".to_string());

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

    // Accumulator client from generated proto
    let mut client =
        numaflow::proto::accumulator::accumulator_client::AccumulatorClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    // Test scenario: Send out-of-order messages and verify they are sorted by event_time
    {
        use numaflow::proto::accumulator as acc_proto;

        let base_time = now_ts().seconds;

        // We'll send messages with event times: t+30, t+10, t+20, t+40
        // And watermarks that advance: t+5, t+15, t+25, t+45
        // Expected output order after sorting: t+10, t+20, t+30, t+40

        // 1. OPEN operation - create window with message at t+30
        let open_req = acc_proto::AccumulatorRequest {
            payload: Some(acc_proto::Payload {
                keys: vec!["key1".into()],
                value: b"msg_at_t30".to_vec(),
                watermark: Some(ts_from_secs(base_time + 5)), // WM at t+5
                event_time: Some(ts_from_secs(base_time + 30)), // Event at t+30
                headers: Default::default(),
                id: "msg1".to_string(),
            }),
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: acc_proto::accumulator_request::window_operation::Event::Open as i32,
                keyed_window: Some(acc_proto::KeyedWindow {
                    start: Some(ts_from_secs(base_time)),
                    end: Some(ts_from_secs(base_time + 60)),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }),
            }),
        };
        tx.send(open_req).await.unwrap();

        // 2. APPEND - message at t+10 with WM at t+15 (should flush t+10)
        let append_req1 = acc_proto::AccumulatorRequest {
            payload: Some(acc_proto::Payload {
                keys: vec!["key1".into()],
                value: b"msg_at_t10".to_vec(),
                watermark: Some(ts_from_secs(base_time + 15)), // WM advances to t+15
                event_time: Some(ts_from_secs(base_time + 10)), // Event at t+10
                headers: Default::default(),
                id: "msg2".to_string(),
            }),
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: acc_proto::accumulator_request::window_operation::Event::Append as i32,
                keyed_window: Some(acc_proto::KeyedWindow {
                    start: Some(ts_from_secs(base_time)),
                    end: Some(ts_from_secs(base_time + 60)),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }),
            }),
        };
        tx.send(append_req1).await.unwrap();

        // 3. APPEND - message at t+20 with WM at t+25 (should flush t+20)
        let append_req2 = acc_proto::AccumulatorRequest {
            payload: Some(acc_proto::Payload {
                keys: vec!["key1".into()],
                value: b"msg_at_t20".to_vec(),
                watermark: Some(ts_from_secs(base_time + 25)), // WM advances to t+25
                event_time: Some(ts_from_secs(base_time + 20)), // Event at t+20
                headers: Default::default(),
                id: "msg3".to_string(),
            }),
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: acc_proto::accumulator_request::window_operation::Event::Append as i32,
                keyed_window: Some(acc_proto::KeyedWindow {
                    start: Some(ts_from_secs(base_time)),
                    end: Some(ts_from_secs(base_time + 60)),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }),
            }),
        };
        tx.send(append_req2).await.unwrap();

        // 4. APPEND - message at t+40 with WM at t+45 (should flush t+30, t+40)
        let append_req3 = acc_proto::AccumulatorRequest {
            payload: Some(acc_proto::Payload {
                keys: vec!["key1".into()],
                value: b"msg_at_t40".to_vec(),
                watermark: Some(ts_from_secs(base_time + 45)), // WM advances to t+45
                event_time: Some(ts_from_secs(base_time + 40)), // Event at t+40
                headers: Default::default(),
                id: "msg4".to_string(),
            }),
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: acc_proto::accumulator_request::window_operation::Event::Append as i32,
                keyed_window: Some(acc_proto::KeyedWindow {
                    start: Some(ts_from_secs(base_time)),
                    end: Some(ts_from_secs(base_time + 60)),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }),
            }),
        };
        tx.send(append_req3).await.unwrap();

        // 5. CLOSE operation - close the window
        let close_req = acc_proto::AccumulatorRequest {
            payload: None,
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: acc_proto::accumulator_request::window_operation::Event::Close as i32,
                keyed_window: Some(acc_proto::KeyedWindow {
                    start: Some(ts_from_secs(base_time)),
                    end: Some(ts_from_secs(base_time + 60)),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }),
            }),
        };
        tx.send(close_req).await.unwrap();
    }

    // Drop sender to signal end-of-stream
    drop(tx);

    // Start the RPC using a Request-wrapped stream
    let request = Request::new(ReceiverStream::new(rx));
    let mut resp = client.accumulate_fn(request).await?.into_inner();

    // Read responses and verify they are in sorted order
    let mut messages = Vec::new();
    let mut found_eof = false;

    while let Some(r) = resp.message().await? {
        if let Some(payload) = r.payload {
            let value = String::from_utf8(payload.value.clone())
                .expect("Payload value should be valid UTF-8");
            println!("Received message: {}", value);
            messages.push(value);
        }

        if r.eof {
            println!("Received EOF");
            found_eof = true;
        }
    }

    // Verify we got all 4 messages in sorted order
    assert_eq!(messages.len(), 4, "Expected 4 messages");
    assert_eq!(messages[0], "msg_at_t10", "First message should be t+10");
    assert_eq!(messages[1], "msg_at_t20", "Second message should be t+20");
    assert_eq!(messages[2], "msg_at_t30", "Third message should be t+30");
    assert_eq!(messages[3], "msg_at_t40", "Fourth message should be t+40");
    assert!(found_eof, "Should have received EOF");

    println!("All messages received in correct sorted order!");

    Ok(())
}

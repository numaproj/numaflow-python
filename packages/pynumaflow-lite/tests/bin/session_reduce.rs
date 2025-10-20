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
        .or_else(|| env::var("NUMAFLOW_SESSION_REDUCE_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/sessionreduce.sock".to_string());

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

    // SessionReduce client from generated proto
    let mut client =
        numaflow::proto::session_reduce::session_reduce_client::SessionReduceClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    // Test scenario: Create a session window, append some data, then close it
    {
        use numaflow::proto::session_reduce as sr_proto;

        // 1. OPEN operation - create a new session window with first message
        let open_req = sr_proto::SessionReduceRequest {
            payload: Some(sr_proto::session_reduce_request::Payload {
                keys: vec!["key1".into()],
                value: b"1".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(sr_proto::session_reduce_request::WindowOperation {
                event: sr_proto::session_reduce_request::window_operation::Event::Open as i32,
                keyed_windows: vec![sr_proto::KeyedWindow {
                    start: Some(now_ts()),
                    end: Some(prost_types::Timestamp {
                        seconds: now_ts().seconds + 60,
                        nanos: 0,
                    }),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }],
            }),
        };
        tx.send(open_req).await.unwrap();

        // 2. APPEND operation - add more data to the same window
        let append_req = sr_proto::SessionReduceRequest {
            payload: Some(sr_proto::session_reduce_request::Payload {
                keys: vec!["key1".into()],
                value: b"2".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(sr_proto::session_reduce_request::WindowOperation {
                event: sr_proto::session_reduce_request::window_operation::Event::Append as i32,
                keyed_windows: vec![sr_proto::KeyedWindow {
                    start: Some(now_ts()),
                    end: Some(prost_types::Timestamp {
                        seconds: now_ts().seconds + 60,
                        nanos: 0,
                    }),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }],
            }),
        };
        tx.send(append_req).await.unwrap();

        // 3. APPEND another message
        let append_req2 = sr_proto::SessionReduceRequest {
            payload: Some(sr_proto::session_reduce_request::Payload {
                keys: vec!["key1".into()],
                value: b"3".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(sr_proto::session_reduce_request::WindowOperation {
                event: sr_proto::session_reduce_request::window_operation::Event::Append as i32,
                keyed_windows: vec![sr_proto::KeyedWindow {
                    start: Some(now_ts()),
                    end: Some(prost_types::Timestamp {
                        seconds: now_ts().seconds + 60,
                        nanos: 0,
                    }),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }],
            }),
        };
        tx.send(append_req2).await.unwrap();

        // 4. CLOSE operation - close the window
        let close_req = sr_proto::SessionReduceRequest {
            payload: None,
            operation: Some(sr_proto::session_reduce_request::WindowOperation {
                event: sr_proto::session_reduce_request::window_operation::Event::Close as i32,
                keyed_windows: vec![sr_proto::KeyedWindow {
                    start: Some(now_ts()),
                    end: Some(prost_types::Timestamp {
                        seconds: now_ts().seconds + 60,
                        nanos: 0,
                    }),
                    slot: "slot-0".to_string(),
                    keys: vec!["key1".into()],
                }],
            }),
        };
        tx.send(close_req).await.unwrap();
    }

    // Drop sender to signal end-of-stream
    drop(tx);

    // Start the RPC using a Request-wrapped stream
    let request = Request::new(ReceiverStream::new(rx));
    let mut resp = client.session_reduce_fn(request).await?.into_inner();

    // Read responses until we see a result and EOF
    let mut found_result = false;
    let mut found_eof = false;

    while let Some(r) = resp.message().await? {
        if let Some(res) = r.result {
            // We should get a count of 3 (three messages appended)
            let count_str =
                String::from_utf8(res.value.clone()).expect("Result value should be valid UTF-8");
            let count: i32 = count_str.parse().expect("Result should be a number");

            println!("Received result: count={}", count);
            assert_eq!(count, 3, "Expected count of 3 messages");
            assert_eq!(res.keys, vec!["key1"], "Expected keys to match");
            found_result = true;
        }

        if r.eof {
            println!("Received EOF");
            found_eof = true;
        }
    }

    assert!(found_result, "Should have received a result");
    assert!(found_eof, "Should have received EOF");

    Ok(())
}

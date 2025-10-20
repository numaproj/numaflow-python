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
        .or_else(|| env::var("NUMAFLOW_REDUCE_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/reduce.sock".to_string());

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

    // Reduce client from generated proto
    let mut client = numaflow::proto::reduce::reduce_client::ReduceClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    // Build a single valid ReduceRequest that includes BOTH payload and window operation
    {
        use numaflow::proto::reduce as reduce_proto;
        let rr = reduce_proto::ReduceRequest {
            payload: Some(reduce_proto::reduce_request::Payload {
                keys: vec!["k".into()],
                value: b"1".to_vec(),
                watermark: Some(now_ts()),
                event_time: Some(now_ts()),
                headers: Default::default(),
            }),
            operation: Some(reduce_proto::reduce_request::WindowOperation {
                event: 0, // not used by server logic currently
                windows: vec![reduce_proto::Window {
                    start: Some(now_ts()),
                    end: Some(now_ts()),
                    slot: "slot-0".to_string(),
                }],
            }),
        };
        tx.send(rr).await.unwrap();
    }

    // Drop sender to signal end-of-stream (COB)
    drop(tx);

    // Start the RPC using a Request-wrapped stream
    let request = Request::new(ReceiverStream::new(rx));
    let mut resp = client.reduce_fn(request).await?.into_inner();

    // Read responses until we see a result; server will also send an EOF after tasks close
    loop {
        if let Some(r) = resp.message().await? {
            if let Some(res) = r.result {
                assert!(!res.value.is_empty());
                break;
            }
        } else {
            panic!("Stream ended without a result");
        }
    }

    Ok(())
}

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, transport::Uri};
use tower::service_fn;

use numaflow::proto::accumulator as acc_proto;
use numaflow::proto::accumulator::accumulator_request::window_operation::Event;

fn ts_from_secs(secs: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: secs,
        nanos: 0,
    }
}

fn keyed_window(base_time: i64) -> acc_proto::KeyedWindow {
    acc_proto::KeyedWindow {
        start: Some(ts_from_secs(base_time)),
        end: Some(ts_from_secs(base_time + 60)),
        slot: "slot-0".to_string(),
        keys: vec!["key1".into()],
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_ACCUMULATOR_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/accumulator.sock".to_string());

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

    let mut client =
        numaflow::proto::accumulator::accumulator_client::AccumulatorClient::new(channel);

    let (tx, rx) = mpsc::channel(16);

    let base_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

    // (id, event_time offset, watermark offset)
    let inputs = [("msg1", 30, 5), ("msg2", 10, 15), ("msg3", 20, 25)];
    let headers = HashMap::from([("h1".to_string(), "v1".to_string())]);

    for (i, (id, et, wm)) in inputs.iter().enumerate() {
        let event = if i == 0 { Event::Open } else { Event::Append };
        tx.send(acc_proto::AccumulatorRequest {
            payload: Some(acc_proto::Payload {
                keys: vec!["key1".into()],
                value: format!("value-{id}").into_bytes(),
                watermark: Some(ts_from_secs(base_time + wm)),
                event_time: Some(ts_from_secs(base_time + et)),
                headers: headers.clone(),
                id: id.to_string(),
            }),
            operation: Some(acc_proto::accumulator_request::WindowOperation {
                event: event as i32,
                keyed_window: Some(keyed_window(base_time)),
            }),
        })
        .await?;
    }

    tx.send(acc_proto::AccumulatorRequest {
        payload: None,
        operation: Some(acc_proto::accumulator_request::WindowOperation {
            event: Event::Close as i32,
            keyed_window: Some(keyed_window(base_time)),
        }),
    })
    .await?;
    drop(tx);

    let request = Request::new(ReceiverStream::new(rx));
    let mut resp = client.accumulate_fn(request).await?.into_inner();

    let mut dropped = Vec::new();
    let mut found_eof = false;

    while let Some(r) = resp.message().await? {
        if r.eof {
            found_eof = true;
            continue;
        }
        assert_eq!(
            r.tags,
            vec![numaflow::shared::DROP.to_string()],
            "Every response should carry the DROP tag"
        );
        let payload = r.payload.expect("Drop response should carry a payload");
        assert!(payload.value.is_empty(), "Drop message value should be empty");
        assert_eq!(payload.keys, vec!["key1".to_string()]);
        assert_eq!(payload.headers, headers);
        dropped.push(payload);
    }

    assert!(found_eof, "Should have received EOF");
    assert_eq!(dropped.len(), inputs.len(), "Expected one drop per datum");

    for (payload, (id, et, wm)) in dropped.iter().zip(inputs.iter()) {
        assert_eq!(payload.id, *id, "Drop message id should match the datum");
        assert_eq!(
            payload.event_time,
            Some(ts_from_secs(base_time + et)),
            "Drop message event_time should match the datum"
        );
        assert_eq!(
            payload.watermark,
            Some(ts_from_secs(base_time + wm)),
            "Drop message watermark should match the datum"
        );
    }

    println!("All datums were dropped with their metadata preserved!");

    Ok(())
}

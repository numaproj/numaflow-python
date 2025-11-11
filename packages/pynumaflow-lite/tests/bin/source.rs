use std::env;
use std::path::PathBuf;

use numaflow::proto;
use numaflow::proto::source::source_client::SourceClient;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::transport::Uri;
use tower::service_fn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow overriding the socket path via first CLI arg or env var.
    let sock_path = env::args()
        .nth(1)
        .or_else(|| env::var("NUMAFLOW_SOURCE_SOCK").ok())
        .unwrap_or_else(|| "/tmp/var/run/numaflow/source.sock".to_string());

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

    let mut client = SourceClient::new(channel);

    // Test 1: Read messages
    println!("Testing read operation...");
    let messages = read_messages(&mut client, 5).await?;
    println!("Read {} messages", messages.len());
    assert_eq!(messages.len(), 5, "Should read exactly 5 messages");

    // Verify message content
    for (i, message) in messages.iter().enumerate() {
        println!(
            "Message {}: payload={}, keys={:?}, partition_id={}",
            i,
            String::from_utf8_lossy(&message.payload),
            message.keys,
            message.offset.as_ref().unwrap().partition_id
        );
        assert!(!message.payload.is_empty(), "Message should have payload");
        assert!(message.offset.is_some(), "Message should have offset");
    }

    // Test 2: Pending before ack
    println!("\nTesting pending operation before ack...");
    let pending_response = client.pending_fn(Request::new(())).await?;
    let pending_count = pending_response.into_inner().result.unwrap().count;
    println!("Pending messages: {}", pending_count);
    // The simple source returns 0 for pending
    assert_eq!(pending_count, 0, "Simple source should return 0 pending");

    // Test 3: Partitions
    println!("\nTesting partitions operation...");
    let partitions_response = client.partitions_fn(Request::new(())).await?;
    let partitions = partitions_response.into_inner().result.unwrap().partitions;
    println!("Partitions: {:?}", partitions);
    assert!(!partitions.is_empty(), "Should have at least one partition");

    // Test 4: Ack messages
    println!("\nTesting ack operation...");
    ack_messages(&mut client, &messages).await?;
    println!("Successfully acknowledged {} messages", messages.len());

    // Test 5: Read more messages and test nack
    println!("\nTesting nack operation...");
    let nack_messages = read_messages(&mut client, 2).await?;
    println!("Read {} messages for nack test", nack_messages.len());
    nack_messages_fn(&mut client, &nack_messages).await?;
    println!("Successfully nacked {} messages", nack_messages.len());

    println!("\nAll source tests passed!");

    Ok(())
}

/// Read messages from the source with proper handshake
async fn read_messages(
    client: &mut SourceClient<tonic::transport::Channel>,
    num_records: u64,
) -> Result<Vec<proto::source::read_response::Result>, Box<dyn std::error::Error>> {
    let (read_tx, read_rx) = mpsc::channel(4);

    // Send handshake
    let handshake_request = proto::source::ReadRequest {
        request: None,
        handshake: Some(proto::source::Handshake { sot: true }),
    };
    read_tx.send(handshake_request).await?;

    // Send read request
    let read_request = proto::source::ReadRequest {
        request: Some(proto::source::read_request::Request {
            num_records,
            timeout_in_ms: 1000,
        }),
        handshake: None,
    };
    read_tx.send(read_request).await?;
    drop(read_tx);

    let mut response_stream = client
        .read_fn(Request::new(ReceiverStream::new(read_rx)))
        .await?
        .into_inner();

    // Expect handshake response
    let handshake_response = response_stream.message().await?.unwrap();
    assert!(
        handshake_response.handshake.is_some(),
        "Should receive handshake response"
    );

    let mut messages = Vec::new();
    while let Some(response) = response_stream.message().await? {
        if let Some(status) = response.status {
            if status.eot {
                break;
            }
        }
        if let Some(result) = response.result {
            messages.push(result);
        }
    }

    Ok(messages)
}

/// Acknowledge messages with proper handshake
async fn ack_messages(
    client: &mut SourceClient<tonic::transport::Channel>,
    messages: &[proto::source::read_response::Result],
) -> Result<(), Box<dyn std::error::Error>> {
    let (ack_tx, ack_rx) = mpsc::channel(10);

    // Send handshake
    let ack_handshake_request = proto::source::AckRequest {
        request: None,
        handshake: Some(proto::source::Handshake { sot: true }),
    };
    ack_tx.send(ack_handshake_request).await?;

    // Send ack requests
    for message in messages {
        let ack_request = proto::source::AckRequest {
            request: Some(proto::source::ack_request::Request {
                offsets: vec![proto::source::Offset {
                    offset: message.offset.as_ref().unwrap().offset.clone(),
                    partition_id: message.offset.as_ref().unwrap().partition_id,
                }],
            }),
            handshake: None,
        };
        ack_tx.send(ack_request).await?;
    }
    drop(ack_tx);

    let mut ack_response = client
        .ack_fn(Request::new(ReceiverStream::new(ack_rx)))
        .await?
        .into_inner();

    // Consume handshake response
    let handshake_response = ack_response.message().await?.unwrap();
    assert!(
        handshake_response.handshake.unwrap().sot,
        "Should receive ack handshake"
    );

    // Consume ack responses
    for _ in 0..messages.len() {
        let ack_result = ack_response.message().await?.unwrap();
        assert!(
            ack_result.result.is_some(),
            "Should receive ack result for each message"
        );
    }

    Ok(())
}

/// Negatively acknowledge messages
async fn nack_messages_fn(
    client: &mut SourceClient<tonic::transport::Channel>,
    messages: &[proto::source::read_response::Result],
) -> Result<(), Box<dyn std::error::Error>> {
    for message in messages {
        let nack_request = proto::source::NackRequest {
            request: Some(proto::source::nack_request::Request {
                offsets: vec![proto::source::Offset {
                    offset: message.offset.as_ref().unwrap().offset.clone(),
                    partition_id: message.offset.as_ref().unwrap().partition_id,
                }],
            }),
        };

        let nack_response = client.nack_fn(Request::new(nack_request)).await?;
        assert!(
            nack_response.into_inner().result.is_some(),
            "Should receive nack result"
        );
    }

    Ok(())
}

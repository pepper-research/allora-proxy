use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

lazy_static::lazy_static! {
    static ref ALLORA_API_KEY: String = std::env::var("ALLORA_API_KEY")
        .expect("ALLORA_API_KEY must be set");
}

const API_INTERVAL: u64 = 1000;
const MAX_CONNECTIONS: usize = 500;
const API_URLS: &[(&str, &str, &str)] = &[
    ("ETH", "5m", "https://api.upshot.xyz/v2/allora/consumer/price/ethereum-11155111/ETH/5m"),
    ("ETH", "8h", "https://api.upshot.xyz/v2/allora/consumer/price/ethereum-11155111/ETH/8h"),
    ("BTC", "5m", "https://api.upshot.xyz/v2/allora/consumer/price/ethereum-11155111/BTC/5m"),
    ("BTC", "8h", "https://api.upshot.xyz/v2/allora/consumer/price/ethereum-11155111/BTC/8h"),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AlloraMessage {
    product: String,
    interval: String,
    data: Value
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_server().await
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await.expect("Failed to bind listener");
    println!("Websocket server listening on ws://0.0.0.0:8080");

    let (tx, _) = broadcast::channel::<String>(16);
    let tx_clone = tx.clone();

    // Track number of active subscribers
    let subscriber_count = Arc::new(AtomicUsize::new(0));
    let subscriber_count_api = subscriber_count.clone();

    let api_interval_handler = tokio::spawn(async move {
        let client = reqwest::Client::new();

        loop {
            // Only make API calls if there are active subscribers
            if subscriber_count_api.load(Ordering::Relaxed) > 0 {
                for (symbol, interval, url) in API_URLS {
                    match async {
                        let response = client.get(url.to_string())
                            .header("accept", "application/json")
                            .header("x-api-key", ALLORA_API_KEY.to_string())
                            .send()
                            .await?;

                        let body = response.json().await?;

                        let msg = AlloraMessage {
                            product: symbol.to_string(),
                            interval: interval.to_string(),
                            data: body
                        };

                        Ok::<_, Box<dyn std::error::Error>>(serde_json::to_string(&msg)?)
                    }.await {
                        Ok(message) => {
                            // Only send if we still have subscribers
                            if subscriber_count_api.load(Ordering::Relaxed) > 0 {
                                // Ignore send errors - this means no active subscribers
                                let _ = tx.send(message);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error fetching/processing API data: {}", e);
                            // Implement exponential backoff or other error handling as needed
                        }
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(API_INTERVAL)).await;
        }
    });

    while let Ok((stream, addr)) = listener.accept().await {
        // Check connection limit
        if subscriber_count.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            println!("Connection limit reached, rejecting client: {:?}", addr);
            continue;
        }

        println!("New client: {:?}", addr);
        let mut rx = tx_clone.subscribe();
        let subscriber_count = subscriber_count.clone();

        // Increment subscriber count
        subscriber_count.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws_stream) => ws_stream,
                Err(e) => {
                    println!("Error during WebSocket handshake: {}", e);
                    subscriber_count.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };

            let (mut write, read) = ws_stream.split();

            // Handle client disconnection
            let mut read = read.fuse();

            loop {
                tokio::select! {
                    message = rx.recv() => {
                        match message {
                            Ok(msg) => {
                                if let Err(e) = write.send(tokio_tungstenite::tungstenite::Message::Text(Utf8Bytes::from(msg))).await {
                                    println!("Error sending message to client: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("Error receiving broadcast message: {}", e);
                                break;
                            }
                        }
                    }
                    // Handle client messages or disconnection
                    msg = read.next() => {
                        match msg {
                            Some(Ok(_)) => (), // Handle client messages if needed
                            Some(Err(e)) => {
                                println!("Error in client connection: {}", e);
                                break;
                            }
                            None => {
                                println!("Client disconnected");
                                break;
                            }
                        }
                    }
                }
            }

            // Decrement subscriber count on disconnect
            subscriber_count.fetch_sub(1, Ordering::Relaxed);
        });
    }

    api_interval_handler.await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::time::Duration;
    use serde_json::json;
    use tokio::net::TcpStream;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

    // Helper function to create a test client
    async fn create_test_client() -> WebSocketStream<MaybeTlsStream<TcpStream>> {
        let (ws_stream, _) = connect_async("ws://localhost:8080")
            .await
            .expect("Failed to connect");
        ws_stream
    }

    #[tokio::test]
    async fn test_websocket_connection() {
        // Start the server in a separate task
        let server_handle = tokio::spawn(async {
            run_server().await.expect("Error starting server");
        });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a test client
        let ws_stream = create_test_client().await;
        let (_, mut read) = ws_stream.split();

        // Verify we can receive a message
        if let Some(msg) = read.next().await {
            assert!(msg.is_ok());
            let msg = msg.unwrap();
            assert!(msg.is_text());
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_broadcast_channel() {
        let (tx, _) = broadcast::channel::<String>(16);
        let mut rx1 = tx.subscribe();
        let mut rx2 = tx.subscribe();

        // Test message
        let test_msg = AlloraMessage {
            product: "ETH".to_string(),
            interval: "5m".to_string(),
            data: json!({"price": "1234.56"})
        };
        let serialized = serde_json::to_string(&test_msg).unwrap();

        // Send message
        tx.send(serialized.clone()).unwrap();

        // Verify both receivers get the message
        assert_eq!(rx1.recv().await.unwrap(), serialized);
        assert_eq!(rx2.recv().await.unwrap(), serialized);
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        // Start server
        let server_handle = tokio::spawn(async {
            run_server().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create multiple clients
        let mut clients = vec![];
        for _ in 0..3 {
            let ws_stream = create_test_client().await;
            clients.push(ws_stream);
        }

        // Split streams and verify each client receives messages
        for mut client in clients {
            let (_, mut read) = client.split();

            if let Some(msg) = read.next().await {
                assert!(msg.is_ok());
                let msg = msg.unwrap();
                assert!(msg.is_text());

                // Verify message format
                let parsed: AlloraMessage = serde_json::from_str::<AlloraMessage>(msg.to_text().unwrap()).unwrap();
                assert!(!parsed.product.is_empty());
                assert!(!parsed.interval.is_empty());
                assert!(parsed.data.is_object());
            }
        }

        server_handle.abort();
    }

    #[test]
    fn test_allora_message_serialization() {
        let msg = AlloraMessage {
            product: "ETH".to_string(),
            interval: "5m".to_string(),
            data: json!({"price": "1234.56"})
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        let deserialized: AlloraMessage = serde_json::from_str(&serialized).unwrap();

        assert_eq!(msg.product, deserialized.product);
        assert_eq!(msg.interval, deserialized.interval);
        assert_eq!(msg.data, deserialized.data);
    }
}
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Utf8Bytes;


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


    let api_interval_handler = tokio::spawn(async move {
        let client = reqwest::Client::new();
        // send reqwest with application/json and api key
        // curl -X 'GET' --url 'https://api.upshot.xyz/v2/allora/consumer/price/ethereum-11155111/ETH/5m' -H 'accept: application/json' -H 'x-api-key: {API_KEY}'
        loop {
            for (symbol, interval, url) in API_URLS {
                let response = client.get(url.to_string())
                    .header("accept", "application/json")
                    .header("x-api-key", ALLORA_API_KEY.to_string())
                    .send()
                    .await
                    .expect("Failed to send request");

                let body = response.json().await.expect("Failed to parse response");

                let msg = AlloraMessage {
                    product: symbol.to_string(),
                    interval: interval.to_string(),
                    data: body
                };
                tx.send(serde_json::to_string(&msg).expect("Error serializing data")).expect("Failed to send message");
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(API_INTERVAL)).await;
        }
    });


    while let Ok((stream, addr)) = listener.accept().await {
        println!("New client: {:?}", addr);
        let mut rx = tx_clone.subscribe();

        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws_stream) => ws_stream,
                Err(e) => {
                    println!("Error during WebSocket handshake: {}", e);
                    return;
                }
            };

            let (mut write, _) = ws_stream.split();


            while let Ok(message) = rx.recv().await {
                write.send(tokio_tungstenite::tungstenite::Message::Text(Utf8Bytes::from(message))).await.expect("Failed to send message");
            }


        });
    }

    api_interval_handler.await.expect("API interval handler failed");
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
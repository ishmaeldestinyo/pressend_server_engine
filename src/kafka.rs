use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("linger.ms", "0")
            .set("acks", "1")
            .set("compression.type", "none")
            .set("batch.size", "1")
             .set("message.max.bytes", "52428800")  // 50MB
            .create()
            .expect("❌ Failed to create Kafka producer");

        Self { producer }
    }

    // fire and forget — returns immediately, no await
    pub fn publish<T: Serialize>(&self, topic: &str, key: &str, payload: &T) {
        let json = match serde_json::to_string(payload) {
            Ok(j) => j,
            Err(e) => {
                println!("[kafka] Serialization error: {}", e);
                return;
            }
        };

        match self.producer.send_result(
            FutureRecord::to(topic).key(key).payload(&json),
        ) {
            Ok(_) => {}
            Err((e, _)) => println!("[kafka] Publish error: {}", e),
        }
    }
}

// ── VerifyEmail event payload
#[derive(Serialize, Deserialize)]
pub struct VerifyEmailEvent {
    pub email: String,
}

pub async fn create_topic_if_not_exists(brokers: &str, topic: &str) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("Failed to create admin client");

    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));

    let result = admin
        .create_topics(&[new_topic], &AdminOptions::new())
        .await;

    match result {
        Ok(_) => println!("[kafka] Topic '{}' created or already exists", topic),
        Err(e) => println!("[kafka] Topic creation error: {}", e),
    }
}
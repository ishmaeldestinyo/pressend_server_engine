use blinq_server::utils::mailer::Mailer;
use blinq_server::worker_handlers::{
    handle_account_loggedin_notification, handle_airtime_purchase,
    handle_change_email, handle_change_password, handle_delete_account, handle_device_update,
    handle_external_transfer, handle_internal_transfer, handle_kyc_upgrade_status,
    handle_legacy_beneficiary_added, handle_legacy_beneficiary_deleted, handle_new_device_login, handle_resend_otp, handle_set_payment_pin, handle_signup,
    handle_suspicious_login, handle_tier2_upgrade, handle_tier3_upgrade, handle_transfer_inflow,
    handle_verify_email,
    handle_data_purchase,
};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    env_logger::init();

    println!("🔧 Blinq Worker starting...");

    // ── Config ────────────────────────────────────────────────────────────────
    let kafka_brokers = std::env::var("KAFKA_BROKER").expect("KAFKA_BROKER must be set");
    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            std::env::var("POSTGRESQL_USER").unwrap(),
            std::env::var("POSTGRESQL_PASSWORD").unwrap(),
            std::env::var("POSTGRESQL_HOST").unwrap(),
            std::env::var("POSTGRESQL_PORT").unwrap_or("5432".into()),
            std::env::var("POSTGRESQL_DBNAME").unwrap(),
        )
    });
    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL must be set");

    let mail_server = std::env::var("MAIL_SERVER").expect("MAIL_SERVER must be set");
    let mail_port: u16 = std::env::var("MAIL_PORT")
        .unwrap_or("465".into())
        .parse()
        .unwrap();
    let mail_user = std::env::var("MAIL_USER").expect("MAIL_USER must be set");
    let mail_password = std::env::var("MAIL_PASSWORD").expect("MAIL_PASSWORD must be set");
    let app_name = std::env::var("APPLICATION_NAME").unwrap_or("Pressend Team".into());

    let topic_new_device = std::env::var("KAFKA_TOPIC_ACCOUNT_NEW_DEVICE").unwrap();

    let topic_tier2_upgrade = std::env::var("KAFKA_TOPIC_TIER2_UPGRADE").unwrap();

    let topic_tier3_upgrade = std::env::var("KAFKA_TOPIC_TIER3_UPGRADE").unwrap();

    let kafka_topic_posthumous_plan_deleted =
        std::env::var("KAFKA_TOPIC_POSTHUMOUS_BENEFICIARY_DELETED").unwrap();

    let kafka_topic_vas_data_requested = std::env::var("KAFKA_TOPIC_VAS_DATA_REQUESTED").unwrap();

    let kafka_topic_internal_transfer_initiated =
        std::env::var("KAFKA_TOPIC_INTERNAL_TRANSFER_INITIATED").unwrap();

    let kafka_topic_external_transfer_initiated =
        std::env::var("KAFKA_TOPIC_EXTERNAL_TRANSFER_INITIATED").unwrap();

    // ── DB pool ───────────────────────────────────────────────────────────────
    let connect_options = PgConnectOptions::from_str(&database_url)
        .expect("❌ Invalid database URL")
        .statement_cache_capacity(0)
        .options([("tcp_keepalives_idle", "60")]);

    let db_pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .idle_timeout(std::time::Duration::from_secs(300))
        .max_lifetime(std::time::Duration::from_secs(1800))
        .connect_with(connect_options)
        .await
        .expect("❌ Worker failed to connect to PostgreSQL");

    // ── Redis ─────────────────────────────────────────────────────────────────
    let redis_client = redis::Client::open(redis_url).expect("❌ Invalid Redis URL");
    let redis = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("❌ Worker failed to connect to Redis");

    println!("✅ Worker DB connected");
    println!("✅ Worker Redis connected");

    // ── Mailer ────────────────────────────────────────────────────────────────
    let support_email = std::env::var("SUPPORT_EMAIL").unwrap_or("support@blinq.com".to_string());

    let mailer = Mailer::new(
        &mail_server,
        mail_port,
        &mail_user,
        &mail_password,
        &app_name,
        &support_email,
    );

    // ── Kafka topic setup ─────────────────────────────────────────────────────
    let topic_signup = std::env::var("KAFKA_TOPIC_ACCOUNT_SIGNUP").unwrap();
    let topic_otp_send = std::env::var("KAFKA_TOPIC_ACCOUNT_OTP_SEND").unwrap();
    let topic_otp_verify = std::env::var("KAFKA_TOPIC_ACCOUNT_OTP_VERIFY").unwrap();

    let topic_password_change = std::env::var("KAFKA_TOPIC_PASSWORD_CHANGE_SUBMIT").unwrap();

    let topic_account_delete = std::env::var("KAFKA_TOPIC_ACCOUNT_DELETE").unwrap();

    let topic_suspicious_login = std::env::var("KAFKA_TOPIC_ACCOUNT_SUSPICIOUS_LOGIN").unwrap();

    let topic_device_update = std::env::var("KAFKA_TOPIC_ACCOUNT_DEVICE_UPDATE").unwrap();

    let kafka_account_login_successful = std::env::var("KAFKA_ACCOUNT_LOGIN_SUCCESSFUL").unwrap();

    let topic_email_change_submit = std::env::var("KAFKA_TOPIC_EMAIL_CHANGE_SUBMIT").unwrap();

    let kafka_topic_vas_airtime_requested =
        std::env::var("KAFKA_TOPIC_VAS_AIRTIME_REQUESTED").unwrap();

    let kafka_topic_payment_pin_set = std::env::var("KAFKA_TOPIC_PAYMENT_PIN_SET").unwrap();

    let topic_kyc_upgrade_status = std::env::var("KAFKA_TOPIC_KYC_UPGRADE_STATUS").unwrap();

    let kafka_topic_transfer_inflow = std::env::var("KAFKA_TOPIC_TRANSFER_INFLOW").unwrap();

    let kafka_topic_legacy_beneficiary_added =
        std::env::var("KAFKA_TOPIC_LEGACYPLAN_BENEFICIARY_ADDED").unwrap();

    let app_cfg = blinq_server::config::Config::from_env();

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .create()
        .expect("❌ Failed to create Kafka admin client");

    let topics_to_create = [
        NewTopic::new(&topic_signup, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_otp_send, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_otp_verify, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_password_change, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_account_delete, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_suspicious_login, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_new_device, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_device_update, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&kafka_topic_transfer_inflow, 1, TopicReplication::Fixed(1)),
        NewTopic::new(
            &kafka_topic_internal_transfer_initiated,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(
            &kafka_account_login_successful,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(&topic_kyc_upgrade_status, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_email_change_submit, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_tier2_upgrade, 1, TopicReplication::Fixed(1)),
        NewTopic::new(&topic_tier3_upgrade, 1, TopicReplication::Fixed(1)),
        NewTopic::new(
            &kafka_topic_legacy_beneficiary_added,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(
            &kafka_topic_external_transfer_initiated,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(
            &kafka_topic_posthumous_plan_deleted,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(
            &kafka_topic_vas_data_requested,
            1,
            TopicReplication::Fixed(1),
        ),
        NewTopic::new(&kafka_topic_payment_pin_set, 1, TopicReplication::Fixed(1)),
        NewTopic::new(
            &kafka_topic_vas_airtime_requested,
            1,
            TopicReplication::Fixed(1),
        ),
    ];

    match admin
        .create_topics(&topics_to_create, &AdminOptions::new())
        .await
    {
        Ok(results) => {
            for r in results {
                match r {
                    Ok(topic) => println!("[kafka] Topic ready: {}", topic),
                    Err((topic, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
                        println!("[kafka] Topic already exists: {}", topic)
                    }
                    Err((topic, e)) => println!("[kafka] Topic create error {}: {}", topic, e),
                }
            }
        }
        Err(e) => println!("[kafka] Admin error: {}", e),
    }

    // ── Kafka consumer ────────────────────────────────────────────────────────
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_brokers)
        .set("group.id", "blinq-worker")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("❌ Failed to create Kafka consumer");

    consumer
        .subscribe(&[
            topic_signup.as_str(),
            topic_otp_send.as_str(),
            topic_otp_verify.as_str(),
            topic_password_change.as_str(),
            topic_account_delete.as_str(),
            topic_suspicious_login.as_str(),
            topic_new_device.as_str(),
            topic_device_update.as_str(),
            kafka_account_login_successful.as_str(),
            topic_email_change_submit.as_str(),
            topic_kyc_upgrade_status.as_str(),
            topic_tier2_upgrade.as_str(),
            topic_tier3_upgrade.as_str(),
            kafka_topic_transfer_inflow.as_str(),
            kafka_topic_legacy_beneficiary_added.as_str(),
            kafka_topic_posthumous_plan_deleted.as_str(),
            kafka_topic_payment_pin_set.as_str(),
            kafka_topic_internal_transfer_initiated.as_str(),
            kafka_topic_external_transfer_initiated.as_str(),
            kafka_topic_vas_airtime_requested.as_str(),
            kafka_topic_vas_data_requested.as_str(),
        ])
        .expect("❌ Failed to subscribe to topics");

    println!("✅ Worker subscribed to Kafka topics");
    println!("🚀 Worker running...");

    // ── Message loop ──────────────────────────────────────────────────────────
    loop {
        match consumer.recv().await {
            Err(e) => println!("[worker] Kafka receive error: {}", e),
            Ok(msg) => {
                let topic = msg.topic().to_string();
                let payload = match msg.payload_view::<str>() {
                    Some(Ok(p)) => p.to_string(),
                    _ => {
                        println!("[worker] Empty or invalid payload");
                        consumer.commit_message(&msg, CommitMode::Async).unwrap();
                        continue;
                    }
                };

                if topic == topic_signup {
                    handle_signup(&payload, &db_pool, &app_cfg, &mut redis.clone(), &mailer).await;
                } else if topic == topic_otp_send {
                    handle_resend_otp(&payload, &redis, &mailer).await;
                } else if topic == topic_otp_verify {
                     handle_verify_email(&payload, &db_pool, &app_cfg, &redis, &mailer).await;
                } else if topic == topic_password_change {
                    handle_change_password(&payload, &db_pool, &mailer).await;
                } else if topic == topic_account_delete {
                    handle_delete_account(&payload, &db_pool, &mailer).await;
                } else if topic == topic_suspicious_login {
                    handle_suspicious_login(&payload, &mailer).await;
                } else if topic == topic_new_device {
                    handle_new_device_login(&payload, &mailer).await;
                } else if topic == topic_device_update {
                    handle_device_update(&payload, &db_pool).await;
                } else if topic == kafka_account_login_successful {
                    handle_account_loggedin_notification(&payload, &mailer).await;
                } else if topic == topic_email_change_submit {
                    handle_change_email(&payload, &db_pool, &mailer).await;
                }else if topic == topic_kyc_upgrade_status {
                    handle_kyc_upgrade_status(&payload, &db_pool, &mailer).await;
                } else if topic == topic_tier2_upgrade {
                    handle_tier2_upgrade(&payload, &db_pool, &app_cfg, &mut redis.clone()).await;
                } else if topic == topic_tier3_upgrade {
                    handle_tier3_upgrade(&payload, &db_pool, &app_cfg, &mut redis.clone()).await;
                } else if topic == kafka_topic_legacy_beneficiary_added {
                    handle_legacy_beneficiary_added(&payload, &db_pool, &mailer).await;
                } else if topic == kafka_topic_posthumous_plan_deleted {
                    handle_legacy_beneficiary_deleted(&payload, &db_pool, &mailer).await;
                } else if topic == kafka_topic_payment_pin_set {
                    handle_set_payment_pin(&payload, &mailer).await;
                } else if topic == kafka_topic_internal_transfer_initiated {
                    handle_internal_transfer(&payload, &db_pool, &app_cfg, &mut redis.clone())
                        .await;
                } else if topic == kafka_topic_transfer_inflow {
                    handle_transfer_inflow(&payload, &db_pool, &app_cfg, &mut redis.clone()).await;
                } else if topic == kafka_topic_external_transfer_initiated {
                    handle_external_transfer(&payload, &db_pool, &app_cfg, &mut redis.clone())
                        .await;
                } else if topic == kafka_topic_vas_airtime_requested {
                    handle_airtime_purchase(&payload, &db_pool, &app_cfg, &mut redis.clone()).await;
                }  else if topic == kafka_topic_vas_data_requested {
                    handle_data_purchase(&payload, &db_pool, &app_cfg, &mut redis.clone()).await;
                } else {
                    println!("[worker] Unknown topic: {}", topic);
                }

                consumer.commit_message(&msg, CommitMode::Async).unwrap();
            }
        }
    }
}

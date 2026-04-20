use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, web};
use blinq_server::utils::mailer::Mailer;
use blinq_server::utils::psb::PsbClient;
use blinq_server::{config, cron, db, kafka, modules, redis};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    const PORT: u16 = 8080;

    env_logger::init();

    let http_client = web::Data::new(reqwest::Client::new());

    let cfg = config::Config::from_env();
    let kafka_cfg = config::KafkaConfig::from_env();

    let mailer = web::Data::new(Mailer::new(
        &cfg.mail_server,
        cfg.mail_port,
        &cfg.mail_user,
        &cfg.mail_password,
        &cfg.app_name,
        &cfg.support_email,
    ));

    let db_pool = db::create_pool(&cfg.database_url()).await;
    let redis = redis::create_client(&cfg.redis_url).await;

    println!("✅ PostgreSQL connected");
    println!("✅ Redis connected");

    let kafka_producer = kafka::KafkaProducer::new(&kafka_cfg.kafka_broker);

    let topics = [
        &kafka_cfg.kafka_topic_account_signup,
        &kafka_cfg.kafka_topic_account_otp_send,
        &kafka_cfg.kafka_topic_account_otp_verify,
        &kafka_cfg.kafka_topic_account_signin,
    ];
    for topic in &topics {
        kafka::create_topic_if_not_exists(&kafka_cfg.kafka_broker, topic).await;
    }

    println!("✅ Kafka connected");

    let cfg = web::Data::new(cfg);
    let kafka_cfg = web::Data::new(kafka_cfg);
    let db_pool = web::Data::new(db_pool);
    let redis = web::Data::new(redis);
    let kafka_producer = web::Data::new(kafka_producer);

    sqlx::migrate!("./migrations")
        .run(db_pool.get_ref())
        .await
        .expect("❌ Failed to run migrations");

    // ── Spawn legacy plan cron ────────────────────────────────────────────────
    cron::legacy_executor::spawn(
        db_pool.get_ref().clone(),
        redis.get_ref().clone(),
        cfg.get_ref().clone(),
    );

    println!("🚀 Blinq Server running on port {}", PORT);

    let psb = web::Data::new(PsbClient::new(&cfg));

    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(cfg.clone())
            .app_data(kafka_cfg.clone())
            .app_data(db_pool.clone())
            .app_data(http_client.clone())
            .app_data(redis.clone())
            .app_data(mailer.clone())
            .app_data(psb.clone())
            .app_data(kafka_producer.clone())
            .app_data(
                web::JsonConfig::default()
                    .limit(20 * 1024 * 1024)
                    .error_handler(|err, _| {
                        let message = err.to_string();
                        actix_web::error::InternalError::from_response(
                            err,
                            HttpResponse::UnprocessableEntity().json(serde_json::json!({
                                "status":  "error",
                                "message": message
                            })),
                        )
                        .into()
                    }),
            )
            .service(
                web::scope("/api")
                    .configure(modules::account::routes::config)
                    .configure(modules::webhook::routes::config)
                    .configure(modules::legacy_plan::routes::config)
                    .configure(modules::transactions::routes::config)
                    .configure(modules::palm::routes::config)
                    .configure(modules::vas::routes::config)
                    .configure(modules::beneficiary::routes::config),
            )
    })
    .bind(format!("0.0.0.0:{}", PORT))?
    .run()
    .await
}

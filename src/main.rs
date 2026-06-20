use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, web};
use blinq_server::utils::mailer::Mailer;
use blinq_server::utils::psb::PsbClient;
use blinq_server::ws::phantom::{create_phantom_channel, phantom_ws};
use blinq_server::utils::dojah::{Config as DojahConfig, DojahClient};
use blinq_server::{config, db, kafka, modules, redis};
use blinq_server::middlewares::governors::{strict_governor, mutating_governor};
use blinq_server::cron::pay_referral_executor::run_referral_reward_payout;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    
    const PORT: u16 = 8080;

    env_logger::init();

    let http_client = web::Data::new(reqwest::Client::new());

    let cfg_env = config::Config::from_env();
    let kafka_cfg_env = config::KafkaConfig::from_env();

    let mailer = web::Data::new(Mailer::new(
        &cfg_env.mail_server,
        cfg_env.mail_port,
        &cfg_env.mail_user,
        &cfg_env.mail_password,
        &cfg_env.app_name,
        &cfg_env.support_email,
    ));

    let db_pool_raw = db::create_pool(&cfg_env.database_url()).await;
    let redis_raw = redis::create_client(&cfg_env.redis_url).await;

    println!("PostgreSQL connected");
    println!("Redis connected");

    let kafka_producer_raw = kafka::KafkaProducer::new(&kafka_cfg_env.kafka_broker);

    let topics = [
        &kafka_cfg_env.kafka_topic_account_signup,
        &kafka_cfg_env.kafka_topic_account_otp_send,
        &kafka_cfg_env.kafka_topic_account_otp_verify,
        &kafka_cfg_env.kafka_topic_account_signin,
    ];
    for topic in &topics {
        kafka::create_topic_if_not_exists(&kafka_cfg_env.kafka_broker, topic).await;
    }

    println!("Kafka connected");

    let cfg = web::Data::new(cfg_env);
    let kafka_cfg = web::Data::new(kafka_cfg_env);
    let db_pool = web::Data::new(db_pool_raw);
    let redis_data = web::Data::new(redis_raw);
    let phantom_tx = web::Data::new(create_phantom_channel());
    let kafka_producer = web::Data::new(kafka_producer_raw);

    let strict_gov = web::Data::new(strict_governor());
    let mutating_gov = web::Data::new(mutating_governor());

    sqlx::migrate!("./migrations")
        .run(db_pool.get_ref())
        .await
        .expect("❌ Failed to run migrations");

    let psb = web::Data::new(PsbClient::new(&cfg));
    let dojah = web::Data::new(DojahClient::new(DojahConfig::new(&cfg.dojah_app_id, &cfg.dojah_sk)));

    // ── Referral reward payout cron — every 60 seconds ───────────────────────
    {
        let cron_db  = db_pool.get_ref().clone();
        let cron_cfg = cfg.get_ref().clone();
        let cron_redis = redis_data.get_ref().clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            let mut redis_conn = cron_redis;
            loop {
                interval.tick().await;
                run_referral_reward_payout(&cron_db, &cron_cfg, &mut redis_conn).await;
            }
        });
    }

    println!("Pressend Server running on port {}", PORT);

    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(cfg.clone())
            .app_data(dojah.clone())
            .app_data(kafka_cfg.clone())
            .app_data(db_pool.clone())
            .app_data(phantom_tx.clone())
            .app_data(http_client.clone())
            .app_data(redis_data.clone())
            .app_data(mailer.clone())
            .app_data(psb.clone())
            .app_data(kafka_producer.clone())
            .app_data(strict_gov.clone())
            .app_data(mutating_gov.clone())
            .app_data(
                web::JsonConfig::default()
                    .limit(100 * 1024 * 1024)
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
                    .route("/ws/phantom", web::get().to(phantom_ws))
                    .configure({
                        let s = strict_gov.clone();
                        let m = mutating_gov.clone();
                        move |sc| modules::account::routes::config(sc, s, m)
                    })
                    .configure({
                        let m = mutating_gov.clone();
                        move |sc| modules::vas::routes::config(sc, m)
                    })
                    .configure({
                        let m = mutating_gov.clone();
                        move |sc| modules::transactions::routes::config(sc, m)
                    })
                    .configure({
                        let m = mutating_gov.clone();
                        move |sc| modules::beneficiary::routes::config(sc, m)
                    })
                    .configure(modules::webhook::routes::config),
            )
    })
    .bind(format!("0.0.0.0:{}", PORT))?
    .run()
    .await
}
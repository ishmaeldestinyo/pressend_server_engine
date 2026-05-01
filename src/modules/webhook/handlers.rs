use actix_web::{HttpRequest, HttpResponse, Responder, web};
use base64::Engine;
use sqlx::PgPool;

use crate::{
    config::KafkaConfig,
    kafka::KafkaProducer,
    modules::webhook::schemas::AccountUpgradeWebhookPayload,
    worker_events::{InboundTransferEvent, KycUpgradeStatusEvent},
};

#[derive(Debug, serde::Deserialize)]
pub struct WebhookQuery {
    pub event: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct TransferWebhookPayload {
    pub merchant: Option<String>,
    pub amount: Option<String>,
    pub sourceaccount: Option<String>,
    pub sourcebank: Option<String>,
    pub sendername: Option<String>,
    pub nipsessionid: Option<String>,
    pub accountnumber: Option<String>,
    pub narration: Option<String>,
    pub transactionref: Option<String>,
    pub orderref: Option<String>,
    pub code: Option<String>,
    pub message: Option<String>,
}

pub async fn _9psb_webhook(
    req: HttpRequest,
    query: web::Query<WebhookQuery>,
    body: web::Json<serde_json::Value>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    db: web::Data<PgPool>,
    cfg: web::Data<Config>,         
) -> impl Responder {

    // ── Basic Auth Verification ───────────────────────────────────────────────
    // TODO: once confirmed, remove the debug log below and set the correct
    //       env var pair (_9PSB_WEBHOOK_USERNAME / _9PSB_WEBHOOK_PASSWORD) if
    //       9PSB uses different creds than the WAAS login.
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Temporary — decode exactly what 9PSB is sending so you can match it
    log::warn!("[webhook] incoming Authorization header: {:?}", auth_header);

    let expected_token = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", cfg.psb_waas_username, cfg.psb_waas_password));
    let expected_header = format!("Basic {}", expected_token);

    if auth_header.is_empty() || auth_header != expected_header {
        log::warn!(
            "[webhook] Unauthorized request — invalid or missing Basic Auth. event={}",
            query.event
        );
        return HttpResponse::Unauthorized().json(serde_json::json!({
            "success": false,
            "message": "Unauthorized"
        }));
    }
    // ─────────────────────────────────────────────────────────────────────────
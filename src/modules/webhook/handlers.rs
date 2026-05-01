use actix_web::{HttpRequest, HttpResponse, Responder, web};
use base64::Engine;
use sqlx::PgPool;

use crate::{
    config::{Config, KafkaConfig},
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

    // Temporary — log everything before auth so we can see what 9PSB sends
    log::warn!("[webhook] incoming event: {}", query.event);
    log::warn!("[webhook] incoming Authorization header: {:?}", req.headers().get("Authorization").and_then(|v| v.to_str().ok()));
    log::warn!("[webhook] incoming body: {:#?}", body);

    // ── Basic Auth Verification (required by 9PSB docs section 9a) ───────────
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

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

    log::info!("[webhook] event={}", query.event);
    log::info!("[webhook] body={:#?}", body);

    let ack = serde_json::json!({
        "success": true,
        "code":    "00",
        "status":  "SUCCESS",
        "message": "Acknowledged"
    });

    // ── Account upgrade ───────────────────────────────────────────────────────
    if query.event == "account-upgrade" {
        let payload: AccountUpgradeWebhookPayload = match serde_json::from_value(body.0.clone()) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("[webhook/account-upgrade] Invalid payload: {}", e);
                return HttpResponse::Ok().json(ack);
            }
        };

        log::info!(
            "[webhook/account-upgrade] ref={} status={} account_number={:?}",
            payload.transaction_tracking_ref,
            payload.status,
            payload.account_number
        );

        // ── Resolve tier from pending_tier_upgrade in DB ──────────────────────
        let account_uuid = uuid::Uuid::parse_str(&payload.transaction_tracking_ref).ok();

        let pending_tier = if let Some(uuid) = account_uuid {
            sqlx::query_scalar!(
                "SELECT pending_tier_upgrade FROM accounts WHERE id = $1 AND deleted_at IS NULL",
                uuid
            )
            .fetch_optional(db.get_ref())
            .await
            .unwrap_or(None)
            .flatten()
            .unwrap_or(1)
        } else {
            1
        };

        let event = KycUpgradeStatusEvent {
            account_id: payload.transaction_tracking_ref.clone(),
            status: payload.status.clone(),
            tier: pending_tier,
            account_number: payload.account_number.clone(),
            account_name: payload.account_name.clone(),
            message: payload.message.clone(),
        };

        kafka.publish(
            &kafka_cfg.kafka_topic_kyc_upgrade_status,
            &payload.transaction_tracking_ref,
            &event,
        );

        return HttpResponse::Ok().json(ack);
    }

    // ── Inbound transfer ──────────────────────────────────────────────────────
    if query.event == "transfer" {
        let payload: TransferWebhookPayload = match serde_json::from_value(body.0.clone()) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("[webhook/transfer] Invalid payload: {}", e);
                return HttpResponse::Ok().json(ack);
            }
        };

        let session_id = payload.nipsessionid.clone().unwrap_or_default();
        let transaction_ref = payload.transactionref.clone().unwrap_or_default();
        let account_number = payload.accountnumber.clone().unwrap_or_default();
        let amount = payload.amount.clone().unwrap_or_default();
        let narration = payload.narration.clone().unwrap_or_default();

        log::info!(
            "[webhook/transfer] session_id={} ref={} account={} amount={}",
            session_id,
            transaction_ref,
            account_number,
            amount
        );

        // ── Fire kafka — ON CONFLICT DO NOTHING in worker is the safety net ───
           let status = payload.message.clone().unwrap_or_default().to_lowercase();
    
        let event = InboundTransferEvent {
            session_id: session_id.clone(),
            transaction_ref,
            amount,
            account_number,
            status,
            sender_name: payload.sendername.clone().unwrap_or_default(),
            sender_account: payload.sourceaccount.clone().unwrap_or_default(),
            sender_bank: payload.sourcebank.clone().unwrap_or_default(),
            narration: payload.narration.clone().unwrap_or_default(),
        };

        kafka.publish(&kafka_cfg.kafka_topic_transfer_inflow, &session_id, &event);

        log::info!(
            "[webhook/transfer] Queued inflow — session_id={}",
            session_id
        );

        return HttpResponse::Ok().json(ack);
    }

    log::warn!("[webhook] Unknown event type: {}", query.event);
    HttpResponse::Ok().json(ack)
}
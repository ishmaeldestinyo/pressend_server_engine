use crate::{
    config::KafkaConfig,
    kafka::KafkaProducer,
    modules::transactions::{
        event::{ExternalTransferInitiatedEvent, InternalTransferInitiatedEvent},
        schemas::{self},
    },
    utils::{
        jwt::AuthUser,
        psb::PsbClient,
        responder::{ApiResponse, ResponseStatus, ValidationErrorResponse},
        transaction_utils::{generate_reference, generate_vas_reference, get_grade, verify_pin},
    },
};
use actix_web::{HttpResponse, Responder, web};
use sqlx::{PgPool, Row};
use uuid::Uuid;
use validator::Validate;

const TTL_1_MIN: u64 = 60;
const TTL_24_HRS: u64 = 86_400;

pub async fn list_mytransaction(
    auth: AuthUser,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    query: web::Query<schemas::TransactionFilterQuery>,
) -> impl Responder {
    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Validate status if provided ───────────────────────────────────────────
    let valid_statuses = ["success", "failed", "reversed", "pending"];
    if let Some(ref s) = query.status {
        if !valid_statuses.contains(&s.as_str()) {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid status. Must be one of: success, failed, reversed, pending"
                    .into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Cache key scoped to filters ───────────────────────────────────────────
    let cache_key = format!(
        "transactions:{}:{}:{}:{}",
        auth.id,
        query.status.as_deref().unwrap_or("all"),
        query.from_interval.as_deref().unwrap_or(""),
        query.to_interval.as_deref().unwrap_or(""),
    );
    let mut redis_conn = redis.get_ref().clone();

    let cached: Option<String> = match redis::cmd("GET")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(_) => None,
    };

    if let Some(data) = cached {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data": serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Build dynamic query ───────────────────────────────────────────────────
    // Show debit rows only when user is sender, credit rows only when user is receiver.
    // This prevents internal transfers from appearing twice on either side.
    let mut builder = sqlx::QueryBuilder::new(
        r#"SELECT
                id, sender_id, reciever_id,
                reciever_account_number, reciever_account_name, reciever_bank,
                reference, type, amount, currency,
                narration, status, channel,
                balance_before, balance_after,
                meta, created_at, updated_at
           FROM transactions
           WHERE ("#,
    );
    builder.push("(sender_id = ");
    builder.push_bind(account_uuid);
    builder.push(" AND type = 'debit')");
    builder.push(" OR ");
    builder.push("(reciever_id = ");
    builder.push_bind(account_uuid);
    builder.push(" AND type = 'credit')");
    builder.push(")");

    if let Some(ref status) = query.status {
        builder.push(" AND status = ");
        builder.push_bind(status.clone());
    }

    if let Some(ref from) = query.from_interval {
        match chrono::NaiveDate::parse_from_str(from, "%Y-%m-%d") {
            Ok(date) => {
                builder.push(" AND created_at >= ");
                builder.push_bind(date.and_hms_opt(0, 0, 0).unwrap());
            }
            Err(_) => {
                return HttpResponse::BadRequest().json(ApiResponse {
                    message: "Invalid from_interval format. Use YYYY-MM-DD".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }
    }

    if let Some(ref to) = query.to_interval {
        match chrono::NaiveDate::parse_from_str(to, "%Y-%m-%d") {
            Ok(date) => {
                builder.push(" AND created_at <= ");
                builder.push_bind(date.and_hms_opt(23, 59, 59).unwrap());
            }
            Err(_) => {
                return HttpResponse::BadRequest().json(ApiResponse {
                    message: "Invalid to_interval format. Use YYYY-MM-DD".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }
    }

    builder.push(" ORDER BY created_at DESC");

    // ── Execute ───────────────────────────────────────────────────────────────
    let rows = builder.build().fetch_all(db.get_ref()).await;

    let transactions = match rows {
        Ok(rows) => rows
            .iter()
            .map(|t| {
                let tx_type = t.get::<String, _>("type");

                serde_json::json!({
                    "id":                      t.get::<uuid::Uuid, _>("id"),
                    "sender_id":               t.get::<Option<uuid::Uuid>, _>("sender_id"),
                    "reciever_id":             t.get::<Option<uuid::Uuid>, _>("reciever_id"),
                    "reciever_account_number": t.get::<Option<String>, _>("reciever_account_number"),
                    "reciever_account_name":   t.get::<Option<String>, _>("reciever_account_name"),
                    "reciever_bank":           t.get::<Option<String>, _>("reciever_bank"),
                    "reference":               t.get::<String, _>("reference"),
                    "type":                    tx_type,
                    "amount":                  t.get::<bigdecimal::BigDecimal, _>("amount").to_string(),
                    "currency":                t.get::<String, _>("currency"),
                    "narration":               t.get::<Option<String>, _>("narration"),
                    "status":                  t.get::<String, _>("status"),
                    "channel":                 t.get::<Option<String>, _>("channel"),
                    "balance_before":          t.get::<Option<bigdecimal::BigDecimal>, _>("balance_before").map(|v| v.to_string()),
                    "balance_after":           t.get::<Option<bigdecimal::BigDecimal>, _>("balance_after").map(|v| v.to_string()),
                    "meta":                    t.get::<Option<serde_json::Value>, _>("meta"),
                    "created_at":              t.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                    "updated_at":              t.get::<chrono::DateTime<chrono::Utc>, _>("updated_at"),
                })
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            println!("[list_mytransaction] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Return 404 if no transactions found ───────────────────────────────────
    if transactions.is_empty() {
        return HttpResponse::NotFound().json(ApiResponse {
            message: "No transactions found".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let data = serde_json::json!({ "transactions": transactions });

    // ── Cache for 3 min ───────────────────────────────────────────────────────
    let _: Result<(), redis::RedisError> = redis::cmd("SETEX")
        .arg(&cache_key)
        .arg(TTL_1_MIN)
        .arg(data.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}


// ── List banks ────────────────────────────────────────────────────────────────
// PSB doc §14: GET /waas/api/v1/get_banks
pub async fn list_bank(
    redis: web::Data<redis::aio::ConnectionManager>,
    psb: web::Data<PsbClient>,
) -> impl Responder {
    let cache_key = "banks:list";
    let mut redis_conn = redis.get_ref().clone();

    // ── Check Redis cache first ───────────────────────────────────────────────
    let cached: Option<String> = match redis::cmd("GET")
        .arg(cache_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(_) => None,
    };

    if let Some(data) = cached {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data": serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Fetch from PSB ────────────────────────────────────────────────────────
    let response = match psb.get(&mut redis_conn, "/waas/api/v1/get_banks").await {
        Ok(res) => res,
        Err(e) => {
            println!("[list_bank] PSB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to fetch banks at this time".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let data = serde_json::json!({ "banks": response });

    // ── Cache for 24 hrs ──────────────────────────────────────────────────────
    let _: Result<(), redis::RedisError> = redis::cmd("SETEX")
        .arg(cache_key)
        .arg(TTL_24_HRS)
        .arg(data.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}

// ── Resolve / verify other bank account detail ────────────────────────────────
// PSB doc §11: POST /waas/api/v1/other_banks_enquiry
// Body: { "customer": { "accountNumber": "...", "bankCode": "..." } }
pub async fn resolve_bank_detail(
    body: web::Json<schemas::ResolveBankDetailRequest>,
    redis: web::Data<redis::aio::ConnectionManager>,
    psb: web::Data<PsbClient>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    // ── Cache key scoped to account_number + bank_code ────────────────────────
    let cache_key = format!("bank_resolve:{}:{}", body.account_number, body.bank_code);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check Redis cache first ───────────────────────────────────────────────
    let cached: Option<String> = match redis::cmd("GET")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(_) => None,
    };

    if let Some(data) = cached {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data": serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Fetch from PSB ────────────────────────────────────────────────────────
    // PSB expects the payload wrapped inside a "customer" object
    let response = match psb
        .post(
            &mut redis_conn,
            "/waas/api/v1/other_banks_enquiry",
            &serde_json::json!({
                "customer": {
                   "account": {
                     "number": body.account_number,
                    "bank":      body.bank_code,
                   }
                }
            }),
        )
        .await
    {
        Ok(res) => res,
        Err(e) => {
            println!("[resolve_bank_detail] PSB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to verify account at this time".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let account = match response["customer"]["account"].as_object() {
        Some(acc) => acc.clone(),
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Unable to resolve account details".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let data = serde_json::json!({
        "account_name":   account.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "account_number": account.get("number").and_then(|v| v.as_str()).unwrap_or(""),
        "bank_code":      account.get("bank").and_then(|v| v.as_str()).unwrap_or(""),
        "bvn":            account.get("bvn").and_then(|v| v.as_str()).unwrap_or(""),
        "kyc":            account.get("kyc").and_then(|v| v.as_str()).unwrap_or(""),
    });

    // ── Cache for 3 min ───────────────────────────────────────────────────────
    let _: Result<(), redis::RedisError> = redis::cmd("SETEX")
        .arg(&cache_key)
        .arg(TTL_1_MIN)
        .arg(data.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}

pub async fn internal_transfer(
    auth: AuthUser,
    body: web::Json<schemas::InternalTransferRequest>,
    db: web::Data<PgPool>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    // ── 1. Validate request fields ────────────────────────────────────────────
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 2. Fetch sender account ───────────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT email, firstname, status, account_number FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account Detail Not Found!".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[internal_transfer] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 3. Account status guard ───────────────────────────────────────────────
    match row.status.as_str() {
        "active" => {}
        "suspended" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Your account has been suspended. Please contact support.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        "deleted" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message:
                    "This account no longer exists. If this was a mistake, please contact support."
                        .into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Account is not active".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── 4. Self-transfer guard ────────────────────────────────────────────────
    let sender_account = row.account_number.clone().unwrap_or_default();

    if sender_account == body.recipient_account {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "You cannot transfer to your own account".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── 5. Recipient must exist — fetch UUID in same query ────────────────────
    let reciever_id = match sqlx::query!(
        "SELECT id FROM accounts WHERE account_number = $1 AND deleted_at IS NULL",
        body.recipient_account
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r.id,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Recipient account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[internal_transfer] Recipient DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 6. Verify PIN ─────────────────────────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    if let Err(response) = verify_pin(&body.pin, account_uuid, db.get_ref(), &mut redis_conn).await
    {
        return response;
    }

    // ── 7. Fire and forget — Kafka only, consumer handles PSB + DB ────────────

    let reference = generate_reference();
    let event = InternalTransferInitiatedEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        sender_account: sender_account.clone(),
        reciever_id: reciever_id.to_string(),
        recipient_account: body.recipient_account.clone(),
        amount: body.amount.to_string(),
        narration: body
            .narration
            .clone()
            .unwrap_or_else(|| "Internal Transfer".into()),
        reference: reference.clone(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_internal_transfer_initiated,
        &row.email,
        &event,
    );

   HttpResponse::Ok().json(serde_json::json!({
        "status": ResponseStatus::SUCCESS,
        "message": "Your recipient should receive the fund shortly",
        "reference": reference 
    }))
}


pub async fn getbank_success_rate(
    body: web::Json<schemas::BankSuccessRateRequest>,
    db: web::Data<PgPool>,
) -> impl Responder {
    // ── 1. Conditional validation ─────────────────────────────────────────────
    if let Some(err_response) = body.validate_fields() {
        return err_response;
    }

    // ── 2. Build query based on channel ───────────────────────────────────────
    let (total, successful): (i64, i64) = if body.is_internal {
        match sqlx::query!(
            r#"
            SELECT
                COUNT(*)                                           AS total,
                COUNT(*) FILTER (WHERE status = 'success')        AS successful
            FROM transactions
            WHERE channel    = 'internal'
              AND status     IN ('success', 'failed')
              AND created_at >= NOW() - INTERVAL '24 hours'
            "#
        )
        .fetch_one(db.get_ref())
        .await
        {
            Ok(r) => (r.total.unwrap_or(0), r.successful.unwrap_or(0)),
            Err(e) => {
                println!("[getbank_success_rate] DB error (internal): {}", e);
                return HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Service temporarily unavailable".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }
    } else {
        let bank_code = body.bank_code.as_deref().unwrap_or("");
        match sqlx::query!(
            r#"
            SELECT
                COUNT(*)                                           AS total,
                COUNT(*) FILTER (WHERE status = 'success')        AS successful
            FROM transactions
            WHERE channel    = 'external'
              AND status     IN ('success', 'failed')
              AND reciever_bank = $1
              AND created_at >= NOW() - INTERVAL '24 hours'
            "#,
            bank_code
        )
        .fetch_one(db.get_ref())
        .await
        {
            Ok(r) => (r.total.unwrap_or(0), r.successful.unwrap_or(0)),
            Err(e) => {
                println!("[getbank_success_rate] DB error (external): {}", e);
                return HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Service temporarily unavailable".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }
    };

    // ── 3. Calculate rate ─────────────────────────────────────────────────────
    let rate: f64 = if total == 0 {
        0.0
    } else {
        (successful as f64 / total as f64) * 100.0
    };

    // ── 4. Grade ──────────────────────────────────────────────────────────────
    let grade = get_grade(rate);

    HttpResponse::Ok().json(serde_json::json!({
        "status": ResponseStatus::SUCCESS,
        "data": {
            "total_settled": total,
            "successful": successful,
            "success_rate": format!("{:.2}%", rate),
            "status": grade["status"]
        }
    }))
}




pub async fn external_transfer(
    auth: AuthUser,
    body: web::Json<schemas::ExternalTransferRequest>,
    db: web::Data<PgPool>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    // ── 1. Validate ───────────────────────────────────────────────────────────
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 2. Fetch sender ───────────────────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT email, firstname, status, account_number, account_name
         FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[external_transfer] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 3. Status guard ───────────────────────────────────────────────────────
    match row.status.as_str() {
        "active" => {}
        "suspended" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Your account has been suspended. Please contact support.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        "deleted" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message:
                    "This account no longer exists. If this was a mistake, please contact support."
                        .into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Account is not active".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }
    

    // ── 4. Must have wallet ───────────────────────────────────────────────────
    let sender_account_number = match row.account_number {
        Some(ref n) => n.clone(),
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 5. Self-transfer guard ────────────────────────────────────────────────
    if sender_account_number == body.recipient_number {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "You cannot transfer to your own account".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── 6. Verify PIN ─────────────────────────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    if let Err(response) = verify_pin(&body.pin, account_uuid, db.get_ref(), &mut redis_conn).await
    {
        return response;
    }

    // ── 7. Fire and forget ────────────────────────────────────────────────────
    let event = ExternalTransferInitiatedEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        sender_account_number: sender_account_number.clone(),
        sender_name: row.account_name.clone().unwrap_or_default(),
        bank_code: body.bank_code.clone(),
        recipient_name: body.recipient_name.clone(),
        recipient_number: body.recipient_number.clone(),
        amount: body.amount.to_string(),
        narration: body.narration.clone().unwrap_or_else(|| "Transfer".into()),
        reference: generate_reference(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_external_transfer_initiated,
        &row.email,
        &event,
    );

    HttpResponse::Ok().json(ApiResponse {
        message: "Your transfer is being processed. You will be notified once it is completed."
            .into(),
        status: ResponseStatus::SUCCESS,
    })
}



pub async fn fund_wallet(
    body: web::Json<schemas::FundWalletRequest>,
    _db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    psb: web::Data<PsbClient>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let mut redis_conn = redis.get_ref().clone();
    let reference = generate_vas_reference();

    let response = match psb
        .post(
            &mut redis_conn,
            "/waas/api/v1/credit/transfer",
            &serde_json::json!({
                "accountNo": body.account_number,
                "narration": body.narration.clone().unwrap_or_else(|| "Wallet Funding".into()),
                "totalAmount": body.amount,
                "transactionId": reference,
                "merchant": {
                    "isFee": false,
                    "merchantFeeAccount": "",
                    "merchantFeeAmount": ""
                }
            }),
        )
        .await
    {
        Ok(res) => res,
        Err(e) => {
            println!("[fund_wallet] PSB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to process funding at this time".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    HttpResponse::Ok().json(serde_json::json!({
        "status": ResponseStatus::SUCCESS,
        "message": "Wallet funded successfully",
        "data": {
            "reference": reference,
            "amount": body.amount,
            "account_number": body.account_number,
            "psb_response": response
        }
    }))
}


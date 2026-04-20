use crate::modules::vas::events::DataPurchaseEvent;
use crate::utils::fms::send_push_notification;
use crate::{
    config::{Config, KafkaConfig},
    kafka::KafkaProducer,
    modules::vas::{events::AirtimePurchaseEvent, schemas},
    utils::{
        jwt::AuthUser,
        responder::{ApiResponse, ResponseStatus, ValidationErrorResponse},
        transaction_utils::{generate_vas_reference, verify_pin},
        vas::VasClient,
    },
};
use actix_web::{HttpResponse, Responder, web};
use sqlx::PgPool;
use sqlx::Row;
use std::str::FromStr;
use uuid::Uuid;

const TTL_1_MIN: u64 = 60;

pub async fn get_phone_network(
    query: web::Query<std::collections::HashMap<String, String>>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let phone = match query.get("phone") {
        Some(p) => p.clone(),
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Phone number is required.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if phone.len() != 11 || !phone.chars().all(|c| c.is_numeric()) {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Please provide a valid 11-digit Nigerian phone number.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Unknown prefix — fall back to VAS API ─────────────────────────────────
    let cache_key = format!("vas:network:{}", phone);
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    match VasClient::new(&cfg)
        .get(
            &mut redis_conn,
            &format!("/vas/api/v1/topup/network?phone={}", phone),
        )
        .await
    {
        Ok(res) => {
            let data = &res["data"];

            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&cache_key)
                .arg(TTL_1_MIN)
                .arg(data.to_string())
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data":   data
            }))
        }
        Err(e) => {
            log::error!("[get_phone_network] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to determine network provider at this time. Please try again."
                    .into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

// ── Network success rates ─────────────────────────────────────────────────────
// Shows success rate per network/provider to help user decide before purchase

pub async fn network_success_rates(
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let cache_key = "vas:success_rates";
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Query success rates per vas_type + network/provider ───────────────────
    let rows = sqlx::query!(
        r#"
        SELECT
            vas_type,
            COALESCE(network, provider)                                         AS provider_name,
            COUNT(*)                                                            AS total,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                AS successful,
            ROUND(
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 100.0
                / NULLIF(COUNT(*), 0), 2
            )                                                                   AS success_rate
        FROM vas_transactions
        WHERE created_at >= NOW() - INTERVAL '7 days'
        GROUP BY vas_type, COALESCE(network, provider)
        ORDER BY vas_type, success_rate DESC
        "#
    )
    .fetch_all(db.get_ref())
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            log::error!("[network_success_rates] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to fetch network status at this time.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Group by vas_type ─────────────────────────────────────────────────────
    let mut airtime: Vec<serde_json::Value> = vec![];
    let mut data: Vec<serde_json::Value> = vec![];
    let mut cable: Vec<serde_json::Value> = vec![];
    let mut power: Vec<serde_json::Value> = vec![];

    for row in &rows {
        let entry = serde_json::json!({
            "provider":      row.provider_name,
            "total":         row.total,
            "successful":    row.successful,
            "success_rate":  row.success_rate,
            "health":        match row.success_rate.as_ref().and_then(|r| r.to_string().parse::<f64>().ok()) {
                Some(r) if r >= 95.0 => "excellent",
                Some(r) if r >= 85.0 => "good",
                Some(r) if r >= 70.0 => "fair",
                _                    => "poor",
            }
        });

        match row.vas_type.as_str() {
            "airtime" => airtime.push(entry),
            "data" => data.push(entry),
            "cable" => cable.push(entry),
            "power" => power.push(entry),
            _ => {}
        }
    }

    // ── If no data yet — return defaults ──────────────────────────────────────
    let default_airtime = || {
        vec![
            serde_json::json!({ "provider": "MTN",     "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
            serde_json::json!({ "provider": "AIRTEL",  "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
            serde_json::json!({ "provider": "GLO",     "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
            serde_json::json!({ "provider": "9MOBILE", "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
        ]
    };

    let default_cable = || {
        vec![
            serde_json::json!({ "provider": "DSTV",    "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
            serde_json::json!({ "provider": "GOTV",    "total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
            serde_json::json!({ "provider": "STARTIMES","total": 0, "successful": 0, "success_rate": null, "health": "unknown" }),
        ]
    };

    let result = serde_json::json!({
        "airtime": if airtime.is_empty() { default_airtime() } else { airtime },
        "data":    if data.is_empty()    { default_airtime() } else { data },
        "cable":   if cable.is_empty()   { default_cable()   } else { cable },
        "power":   if power.is_empty()   { power             } else { power },
        "period":  "last 7 days",
    });

    // ── Cache for 1 min ───────────────────────────────────────────────────────
    let _: Result<(), _> = redis::cmd("SETEX")
        .arg(cache_key)
        .arg(TTL_1_MIN)
        .arg(result.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data":   result
    }))
}

pub async fn airtime_purchase(
    auth: AuthUser,
    body: web::Json<schemas::AirtimePurchaseRequest>,
    db: web::Data<PgPool>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    use validator::Validate;

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

    // ── 2. Fetch account_number ───────────────────────────────────────────────
    let account_number = match sqlx::query_scalar!(
        "SELECT account_number FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(Some(n))) => n,
        Ok(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[airtime_purchase] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 3. Verify PIN (cached in Redis) ───────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    if let Err(response) = verify_pin(&body.pin, account_uuid, db.get_ref(), &mut redis_conn).await
    {
        return response;
    }

    // ── 4. Fire and forget — Kafka only ───────────────────────────────────────
    let event = AirtimePurchaseEvent {
        account_id: auth.id.clone(),
        account_number: account_number.clone(),
        phone_number: body.phone_number.clone(),
        network: body.network.to_uppercase(),
        amount: body.amount.to_string(),
        reference: generate_vas_reference(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_vas_airtime_requested,
        &auth.id,
        &event,
    );

    HttpResponse::Ok().json(ApiResponse {
        message: "Your airtime purchase is being processed. You will be notified shortly.".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn get_topup_status(
    query: web::Query<std::collections::HashMap<String, String>>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    // ── Validate transReference ───────────────────────────────────────────────
    let trans_reference = match query.get("transReference") {
        Some(r) => r.clone(),
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Transaction reference is required.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if trans_reference.is_empty() {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Transaction reference cannot be empty.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let cache_key = format!("vas:topup:status:{}", trans_reference);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache first ─────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Call VAS ──────────────────────────────────────────────────────────────
    match VasClient::new(&cfg)
        .get(
            &mut redis_conn,
            &format!(
                "/vas/api/v1/topup/status?transReference={}",
                trans_reference
            ),
        )
        .await
    {
        Ok(res) => {
            let data = &res["data"];
            let transaction_status = data["transactionStatus"]
                .as_str()
                .unwrap_or("")
                .to_lowercase();

            // ── Cache based on status ─────────────────────────────────────────
            let ttl = match transaction_status.as_str() {
                "success" => 300u64, // 5 min — terminal state, safe to cache longer
                "pending" => 15u64,  // 15s — recheck soon
                _ => 15u64,          // failed/unknown — recheck soon too
            };

            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&cache_key)
                .arg(ttl)
                .arg(data.to_string())
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data":   data
            }))
        }
        Err(e) => {
            log::error!("[get_topup_status] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to retrieve transaction status at this time. Please try again."
                    .into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn get_my_vas_transactions(
    auth: AuthUser,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    query: web::Query<schemas::VasTransactionFilterQuery>,
) -> impl Responder {
    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Cache key scoped to filters ───────────────────────────────────────────
    let cache_key = format!(
        "vas:transactions:{}:{}:{}",
        auth.id,
        query.vas_type.as_deref().unwrap_or("all"),
        query.network.as_deref().unwrap_or("all"),
    );
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Build dynamic query ───────────────────────────────────────────────────
    let mut builder = sqlx::QueryBuilder::new(
        r#"SELECT id, vas_type, network, provider, recipient, amount, currency,
                  reference, debit_account, status, response_code, response_message,
                  meta, created_at, updated_at
           FROM vas_transactions
           WHERE account_id = "#,
    );
    builder.push_bind(account_uuid);

    if let Some(ref vas_type) = query.vas_type {
        builder.push(" AND vas_type = ");
        builder.push_bind(vas_type.clone());
    }

    if let Some(ref network) = query.network {
        builder.push(" AND network = ");
        builder.push_bind(network.to_uppercase());
    }

    builder.push(" ORDER BY created_at DESC");

    let rows = builder.build().fetch_all(db.get_ref()).await;

    let transactions = match rows {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id":               r.get::<uuid::Uuid, _>("id"),
                    "vas_type":         r.get::<String, _>("vas_type"),
                    "network":          r.get::<Option<String>, _>("network"),
                    "provider":         r.get::<Option<String>, _>("provider"),
                    "recipient":        r.get::<String, _>("recipient"),
                    "amount":           r.get::<bigdecimal::BigDecimal, _>("amount").to_string(),
                    "currency":         r.get::<String, _>("currency"),
                    "reference":        r.get::<String, _>("reference"),
                    "debit_account":    r.get::<String, _>("debit_account"),
                    "status":           r.get::<String, _>("status"),
                    "response_code":    r.get::<Option<String>, _>("response_code"),
                    "response_message": r.get::<Option<String>, _>("response_message"),
                    "meta":             r.get::<Option<serde_json::Value>, _>("meta"),
                    "created_at":       r.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                    "updated_at":       r.get::<chrono::DateTime<chrono::Utc>, _>("updated_at"),
                })
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            log::error!("[get_my_vas_transactions] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if transactions.is_empty() {
        return HttpResponse::NotFound().json(ApiResponse {
            message: "No transactions found.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let data = serde_json::json!({ "transactions": transactions });

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data":   data
    }))
}

pub async fn get_data_plans(
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let cache_key = "vas:data_plans:all";
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ────────────────────────────────────────────────
    if let Ok(Some(cached)) = redis::cmd("GET")
        .arg(cache_key)
        .query_async::<_, Option<String>>(&mut redis_conn)
        .await
    {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&cached) {
            return HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "source": "cache",
                "data": parsed
            }));
        }
    }

    // ── Representative numbers per network ─────────────────────────
    let networks = vec![
        ("MTN", "08031234567"),
        ("AIRTEL", "08021234567"),
        ("GLO", "08051234567"),
        ("9MOBILE", "08091234567"),
    ];

    let mut all_plans: Vec<serde_json::Value> = Vec::new();

    // ── Fetch all networks ─────────────────────────────────────────
    for (network, phone) in networks {
        match VasClient::new(&cfg)
            .get(
                &mut redis_conn,
                &format!("/vas/api/v1/topup/dataPlans?phone={}", phone),
            )
            .await
        {
            Ok(res) => {
                if let Some(plans) = res.get("data").and_then(|d| d.as_array()) {
                    for plan in plans {
                        let mut plan_with_network = plan.clone();

                        // Optional: tag each plan with network
                        if let Some(obj) = plan_with_network.as_object_mut() {
                            obj.insert(
                                "network".to_string(),
                                serde_json::Value::String(network.to_string()),
                            );
                        }

                        all_plans.push(plan_with_network);
                    }
                }
            }
            Err(e) => {
                log::error!("[get_data_plans] {} fetch failed: {}", network, e);
                continue; // don't fail entire request
            }
        }
    }

    // ── Cache combined result (3 mins) ─────────────────────────────
    let _: Result<(), _> = redis::cmd("SETEX")
        .arg(cache_key)
        .arg(600u64)
        .arg(serde_json::to_string(&all_plans).unwrap())
        .query_async(&mut redis_conn)
        .await;

    // ── Return combined plans ──────────────────────────────────────
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "source": "api",
        "data": all_plans
    }))
}

pub async fn data_purchase(
    auth: AuthUser,
    body: web::Json<schemas::DataPurchaseRequest>,
    db: web::Data<PgPool>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    use validator::Validate;

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

    // ── 2. Fetch account_number ───────────────────────────────────────────────
    let account_number = match sqlx::query_scalar!(
        "SELECT account_number FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(Some(n))) => n,
        Ok(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[data_purchase] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 3. Verify PIN (Redis cached) ──────────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    if let Err(response) = verify_pin(&body.pin, account_uuid, db.get_ref(), &mut redis_conn).await
    {
        return response;
    }

    // ── 4. Fire and forget — Kafka only ───────────────────────────────────────
    let event = DataPurchaseEvent {
        account_id: auth.id.clone(),
        account_number: account_number.clone(),
        phone_number: body.phone_number.clone(),
        network: body.network.to_uppercase(),
        product_id: body.product_id.clone(),
        amount: body.amount.to_string(),
        reference: generate_vas_reference(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_vas_data_requested, &auth.id, &event);

    HttpResponse::Ok().json(ApiResponse {
        message: "Your data purchase is being processed. You will be notified shortly.".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn get_bill_categories(
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let cache_key = "vas:bill_categories";
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    match VasClient::new(&cfg)
        .get(&mut redis_conn, "/vas/api/v1/billspayment/categories")
        .await
    {
        Ok(res) => {
            let data = &res["data"];

            // ── Cache for 24 hrs — categories rarely change ───────────────────
            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(cache_key)
                .arg(86_400u64)
                .arg(data.to_string())
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data":   data
            }))
        }
        Err(e) => {
            log::error!("[get_bill_categories] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to retrieve bill categories at this time. Please try again."
                    .into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn get_category_billers(
    path: web::Path<String>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let category_id = path.into_inner();
    let cache_key = format!("vas:billers:{}", category_id);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    match VasClient::new(&cfg)
        .get(
            &mut redis_conn,
            &format!("/vas/api/v1/billspayment/billers/{}", category_id),
        )
        .await
    {
        Ok(res) => {
            let data = &res["data"];

            // ── Cache for 24 hrs ──────────────────────────────────────────────
            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&cache_key)
                .arg(86_400u64)
                .arg(data.to_string())
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data":   data
            }))
        }
        Err(e) => {
            log::error!("[get_category_billers] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to retrieve billers at this time. Please try again.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn get_biller_fields(
    path: web::Path<String>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let biller_id = path.into_inner();
    let cache_key = format!("vas:biller_fields:{}", biller_id);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    match VasClient::new(&cfg)
        .get(
            &mut redis_conn,
            &format!("/vas/api/v1/billspayment/fields/{}", biller_id),
        )
        .await
    {
        Ok(res) => {
            let data = &res["data"];

            // ── Cache for 24 hrs ──────────────────────────────────────────────
            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&cache_key)
                .arg(86_400u64)
                .arg(data.to_string())
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data":   data
            }))
        }
        Err(e) => {
            log::error!("[get_biller_fields] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to retrieve biller fields at this time. Please try again.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn get_cabletv_fields(
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let biller_ids = vec!["CW-DSTV", "CW-GOTV", "CW-STARTIMES"];
    let mut redis_conn = redis.get_ref().clone();

    let mut results = serde_json::Map::new();

    for biller_id in biller_ids {
        let cache_key = format!("vas:biller_fields:{}", biller_id);

        // ── Check cache ───────────────────────────────────────────────────────
        let cached: Option<String> = match redis::cmd("GET")
            .arg(&cache_key)
            .query_async(&mut redis_conn)
            .await
        {
            Ok(v) => v,
            Err(_) => None,
        };

        if let Some(data) = cached {
            let parsed = serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default();
            results.insert(biller_id.to_lowercase(), parsed);
            continue;
        }

        // ── Fetch from VAS ────────────────────────────────────────────────────
        match VasClient::new(&cfg)
            .get(
                &mut redis_conn,
                &format!("/vas/api/v1/billspayment/fields/{}", biller_id),
            )
            .await
        {
            Ok(res) => {
                let data = &res["data"];

                // ── Cache for 24 hrs ──────────────────────────────────────────
                let _: Result<(), _> = redis::cmd("SETEX")
                    .arg(&cache_key)
                    .arg(86_400u64)
                    .arg(data.to_string())
                    .query_async(&mut redis_conn)
                    .await;

                results.insert(biller_id.to_lowercase(), data.clone());
            }
            Err(e) => {
                log::error!("[get_biller_fields] VAS error for {}: {}", biller_id, e);
                results.insert(
                    biller_id.to_lowercase(),
                    serde_json::json!({ "error": "Unable to retrieve biller fields" }),
                );
            }
        }
    }

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": results
    }))
}

pub async fn validate_biller(
    body: web::Json<schemas::ValidateBillerRequest>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    use validator::Validate;

    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let mut redis_conn = redis.get_ref().clone();

    // ── Build cache key from biller_id + customer_id (+ optional fields) ─────
    let cache_key = format!(
        "vas:validate_biller:{}:{}:{}:{}",
        body.biller_id,
        body.customer_id,
        body.item_id.as_deref().unwrap_or(""),
        body.amount.as_deref().unwrap_or(""),
    );

    // ── Check cache ───────────────────────────────────────────────────────────
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
            "data":   serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    let mut vas_payload = serde_json::json!({
        "billerId":   body.biller_id,
        "customerId": body.customer_id,
    });

    // ── Optional fields ───────────────────────────────────────────────────────
    if let Some(ref item_id) = body.item_id {
        vas_payload["itemId"] = serde_json::Value::String(item_id.clone());
    }
    if let Some(ref amount) = body.amount {
        vas_payload["amount"] = serde_json::Value::String(amount.clone());
    }
    if let Some(ref firstname) = body.firstname {
        vas_payload["firstname"] = serde_json::Value::String(firstname.clone());
    }
    if let Some(ref lastname) = body.lastname {
        vas_payload["lastname"] = serde_json::Value::String(lastname.clone());
    }

    match VasClient::new(&cfg)
        .post(
            &mut redis_conn,
            "/vas/api/v1/billspayment/validate",
            &vas_payload,
        )
        .await
    {
        Ok(res) => {
            let status = res["status"].as_str().unwrap_or("").to_uppercase();

            if status == "SUCCESS" {
                let data = &res["data"];

                // ── Cache for 1 hr (validation results are short-lived) ───────
                let _: Result<(), _> = redis::cmd("SETEX")
                    .arg(&cache_key)
                    .arg(3_600u64)
                    .arg(data.to_string())
                    .query_async(&mut redis_conn)
                    .await;

                HttpResponse::Ok().json(serde_json::json!({
                    "status": "success",
                    "data":   data
                }))
            } else {
                HttpResponse::BadRequest().json(ApiResponse {
                    message: res["message"]
                        .as_str()
                        .unwrap_or("Unable to validate customer details. Please check your input and try again.")
                        .into(),
                    status: ResponseStatus::ERROR,
                })
            }
        }
        Err(e) => {
            log::error!("[validate_biller] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to validate biller details at this time. Please try again.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn bills_payment(
    auth: AuthUser,
    body: web::Json<schemas::BillsPaymentRequest>,
    db: web::Data<PgPool>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    use validator::Validate;

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

    // ── 2. Fetch account_number ───────────────────────────────────────────────
    let account_number = match sqlx::query_scalar!(
        "SELECT account_number FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(Some(n))) => n,
        Ok(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[bills_payment] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── 3. Verify PIN ─────────────────────────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    if let Err(response) = verify_pin(&body.pin, account_uuid, db.get_ref(), &mut redis_conn).await
    {
        return response;
    }

    // ── 4. Build reference & amount ───────────────────────────────────────────
    let reference = generate_vas_reference();
    let amount_bd = bigdecimal::BigDecimal::from_str(&body.amount).unwrap_or_default();

    let item_id = body.item_id.clone().unwrap_or_default();
    let customer_phone = body.customer_phone.clone().unwrap_or_default();
    let other_field = body.other_field.clone().unwrap_or_default();

    // ── 5. Detect vas_type from biller_id ─────────────────────────────────────
    // Cable TV billers all use the "CW-" prefix (CW-DSTV, CW-GOTV, CW-STARTIMES)
    // Everything else going through this endpoint is a power/electricity biller
    let vas_type = if body.biller_id.starts_with("CW-") {
        "cable"
    } else {
        "power"
    };

    // ── 6. Call VAS ───────────────────────────────────────────────────────────
    let vas_payload = serde_json::json!({
        "customerId":           body.customer_id,
        "billerId":             body.biller_id,
        "itemId":               item_id,
        "customerPhone":        customer_phone,
        "customerName":         body.customer_name,
        "otherField":           other_field,
        "amount":               body.amount,
        "debitAccount":         account_number,
        "transactionReference": reference,
    });

    let vas_result = VasClient::new(&cfg)
        .post(
            &mut redis_conn,
            "/vas/api/v1/billspayment/pay",
            &vas_payload,
        )
        .await;

    // ── 7. Handle VAS response ────────────────────────────────────────────────
    let (tx_status, response_code, response_message, vas_data) = match vas_result {
        Ok(res) => {
            let vas_status = res["status"].as_str().unwrap_or("").to_uppercase();
            let tx_status = match vas_status.as_str() {
                "SUCCESS" => "success",
                "PENDING" => "pending",
                _ => "failed",
            };
            let code = res["responseCode"].as_str().unwrap_or("").to_string();
            let message = res["message"].as_str().unwrap_or("").to_string();
            let data = res["data"].clone();
            (tx_status, code, message, Some(data))
        }
        Err(e) => {
            log::error!(
                "[bills_payment] VAS error — ref: {}, error: {}",
                reference,
                e
            );
            ("failed", String::new(), e.to_string(), None)
        }
    };

    // ── 8. Persist to vas_transactions ────────────────────────────────────────
    let meta = serde_json::json!({
        "biller_id":      body.biller_id,
        "item_id":        item_id,
        "customer_name":  body.customer_name,
        "customer_phone": customer_phone,
        "other_field":    other_field,
        "is_token":       vas_data.as_ref().and_then(|d| d["isToken"].as_bool()),
        "token":          vas_data.as_ref().and_then(|d| d["token"].as_str()),
    });

    if let Err(e) = sqlx::query!(
        r#"
        INSERT INTO vas_transactions
            (account_id, vas_type, provider, recipient, amount, currency,
             reference, debit_account, status, response_code,
             response_message, meta)
        VALUES
            ($1, $2, $3, $4, $5, 'NGN', $6, $7, $8, $9, $10, $11)
        ON CONFLICT (reference) DO NOTHING
        "#,
        account_uuid,
        vas_type, // ← dynamic: 'cable' for CW-* billers, 'power' for everything else
        body.biller_id,
        body.customer_id,
        amount_bd,
        reference,
        account_number,
        tx_status,
        response_code,
        response_message,
        meta,
    )
    .execute(db.get_ref())
    .await
    {
        log::error!("[bills_payment] DB insert error: {}", e);
    }

    // ── 9. Push notification ──────────────────────────────────────────────────
    let device_token = sqlx::query_scalar!(
        "SELECT device_token FROM accounts WHERE id = $1",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    .ok()
    .flatten()
    .flatten();

    if let Some(token) = device_token {
        let (title, body_text) = match tx_status {
            "success" => (
                "Bill Payment Successful",
                format!(
                    "Your bill payment of ₦{} to {} was completed successfully. Ref: {}.",
                    body.amount, body.biller_id, reference
                ),
            ),
            "pending" => (
                "Bill Payment Pending",
                format!(
                    "Your bill payment of ₦{} to {} is being processed. Ref: {}.",
                    body.amount, body.biller_id, reference
                ),
            ),
            _ => (
                "Bill Payment Unsuccessful",
                format!(
                    "We could not process your bill payment of ₦{} to {}. Please try again. Ref: {}.",
                    body.amount, body.biller_id, reference
                ),
            ),
        };

        send_push_notification(
            &token,
            title,
            &body_text,
            Some(serde_json::json!({
                "route":     "NotificationDetail",
                "service":   "bill_payment",
                "status":    tx_status,
                "amount":    body.amount,
                "reference": reference,
                "narration": format!("Bill payment · {}", body.biller_id),
            })),
        )
        .await;
    }

    // ── 10. Return response to client ─────────────────────────────────────────
    match tx_status {
        "success" => HttpResponse::Ok().json(serde_json::json!({
            "status":    "success",
            "message":   format!(
                "Bill payment of ₦{} to {} was successful. Reference: {}.",
                body.amount, body.biller_id, reference
            ),
            "data": {
                "reference":      reference,
                "biller_id":      body.biller_id,
                "customer_id":    body.customer_id,
                "customer_name":  body.customer_name,
                "amount":         body.amount,
                "status":         "success",
                "response_code":  response_code,
                "token":          vas_data.as_ref().and_then(|d| d["token"].as_str()),
                "is_token":       vas_data.as_ref().and_then(|d| d["isToken"].as_bool()),
            }
        })),
        "pending" => HttpResponse::Accepted().json(serde_json::json!({
            "status":  "pending",
            "message": "Your bill payment is being processed. You will be notified shortly.",
            "data": {
                "reference":   reference,
                "biller_id":   body.biller_id,
                "customer_id": body.customer_id,
                "amount":      body.amount,
                "status":      "pending",
            }
        })),
        _ => HttpResponse::BadGateway().json(serde_json::json!({
            "status":  "error",
            "message": format!(
                "Bill payment failed. Please try again or contact support. Reference: {}.",
                reference
            ),
            "data": {
                "reference":      reference,
                "biller_id":      body.biller_id,
                "customer_id":    body.customer_id,
                "amount":         body.amount,
                "status":         "failed",
                "response_code":  response_code,
            }
        })),
    }
}

pub async fn get_bills_payment_status(
    query: web::Query<std::collections::HashMap<String, String>>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let trans_reference = match query.get("transReference") {
        Some(r) => r.clone(),
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Transaction reference is required.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if trans_reference.is_empty() {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Transaction reference cannot be empty.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let mut redis_conn = redis.get_ref().clone();

    match VasClient::new(&cfg)
        .get(
            &mut redis_conn,
            &format!(
                "/vas/api/v1/billspayment/status?transReference={}",
                trans_reference
            ),
        )
        .await
    {
        Ok(res) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data":   res["data"]
        })),
        Err(e) => {
            log::error!("[get_bills_payment_status] VAS error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to retrieve payment status at this time. Please try again.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

use actix_web::{web, HttpResponse, Responder};
use sqlx::{PgPool, Postgres, QueryBuilder};
use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use validator::Validate;
use crate::middlewares::admin_auth::{AdminAuth, require_role};
use crate::modules::admin::transactions::schemas::*;
use crate::utils::responder::{ApiResponse, ResponseStatus};


const READ_ROLES: &[&str] = &["admin", "support", "compliance"];

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct TransactionRow {
    pub id: uuid::Uuid,
    pub sender_id: uuid::Uuid,
    pub reciever_id: Option<uuid::Uuid>,
    pub reciever_account_number: Option<String>,
    pub reciever_account_name: Option<String>,
    pub reciever_bank: Option<String>,
    pub reference: String,
    #[sqlx(rename = "type")]
    pub tx_type: String,
    pub amount: BigDecimal,
    pub currency: String,
    pub narration: Option<String>,
    pub status: String,
    pub channel: Option<String>,
    pub balance_before: Option<BigDecimal>,
    pub balance_after: Option<BigDecimal>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

const TX_COLUMNS: &str = "id, sender_id, reciever_id, reciever_account_number, \
    reciever_account_name, reciever_bank, reference, type, amount, currency, \
    narration, status, channel, balance_before, balance_after, created_at, updated_at";

/// GET /admin/transactions
pub async fn list_transactions(
    admin: AdminAuth,
    query: web::Query<ListTransactionsQuery>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }
    if let Err(errors) = query.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error", "message": "Invalid input", "errors": errors
        }));
    }

    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * limit;

    // Allowlisted sort — never interpolate query.sort_by/order directly.
    let sort_col = match query.sort_by.as_deref() {
        Some("amount") => "amount",
        Some("created_at") | None => "created_at",
        _ => return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: "sort_by must be 'amount' or 'created_at'".into(),
            status: ResponseStatus::ERROR,
        }),
    };
    let order = match query.order.as_deref() {
        Some("asc") => "ASC",
        Some("desc") | None => "DESC",
        _ => return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: "order must be 'asc' or 'desc'".into(),
            status: ResponseStatus::ERROR,
        }),
    };

    let mut qb: QueryBuilder<Postgres> =
        QueryBuilder::new(format!("SELECT {} FROM transactions WHERE 1=1", TX_COLUMNS));

    if let Some(channel) = &query.channel {
        qb.push(" AND channel = ").push_bind(channel);
    }
    if let Some(status) = &query.status {
        qb.push(" AND status = ").push_bind(status);
    }
    if let Some(q) = &query.q {
        qb.push(" AND reference ILIKE ").push_bind(format!("%{}%", q));
    }

    qb.push(format!(" ORDER BY {} {}", sort_col, order));
    qb.push(" LIMIT ").push_bind(limit);
    qb.push(" OFFSET ").push_bind(offset);

    match qb.build_query_as::<TransactionRow>().fetch_all(db.get_ref()).await {
        Ok(rows) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success", "page": page, "limit": limit, "data": rows
        })),
        Err(e) => {
            log::error!("[list_transactions] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/transactions/{id} — full detail + resolution if any
pub async fn get_transaction_details(
    admin: AdminAuth,
    path: web::Path<String>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let tx_id = match uuid::Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().json(ApiResponse {
            message: "Invalid transaction id".into(),
            status: ResponseStatus::ERROR,
        }),
    };

    let row = sqlx::query!(
        r#"SELECT t.id, t.sender_id, t.reciever_id, t.reciever_account_number,
               t.reciever_account_name, t.reciever_bank, t.reference, t.type,
               t.amount, t.currency, t.narration, t.status, t.channel,
               t.balance_before, t.balance_after, t.created_at, t.updated_at,
               r.id as "resolution_id?", r.reason as "resolution_reason?",
               r.resolution_note as "resolution_note?", r.status as "resolution_status?",
               r.resolved_by as "resolved_by?"
           FROM transactions t
           LEFT JOIN resolutions r ON r.transaction_id = t.id
           WHERE t.id = $1"#,
        tx_id
    )
    .fetch_optional(db.get_ref())
    .await;

    match row {
        Ok(Some(r)) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data": {
                "id": r.id, "sender_id": r.sender_id, "reciever_id": r.reciever_id,
                "reciever_account_number": r.reciever_account_number,
                "reciever_account_name": r.reciever_account_name,
                "reciever_bank": r.reciever_bank, "reference": r.reference,
                "type": r.r#type, "amount": r.amount, "currency": r.currency,
                "narration": r.narration, "status": r.status, "channel": r.channel,
                "balance_before": r.balance_before, "balance_after": r.balance_after,
                "created_at": r.created_at, "updated_at": r.updated_at,
                "resolution": r.resolution_id.map(|_| serde_json::json!({
                    "id": r.resolution_id,
                    "reason": r.resolution_reason,
                    "note": r.resolution_note,
                    "status": r.resolution_status,
                    "resolved_by": r.resolved_by,
                }))
            }
        })),
        Ok(None) => HttpResponse::NotFound().json(ApiResponse {
            message: "Transaction not found".into(),
            status: ResponseStatus::ERROR,
        }),
        Err(e) => {
            log::error!("[get_transaction_details] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/transactions/top — highest-value transactions
pub async fn top_transactions(
    admin: AdminAuth,
    query: web::Query<TopTransactionsQuery>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let limit = query.limit.unwrap_or(10).clamp(1, 100);

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "SELECT {} FROM transactions WHERE status = 'success'", TX_COLUMNS
    ));
    if let Some(channel) = &query.channel {
        qb.push(" AND channel = ").push_bind(channel);
    }
    qb.push(" ORDER BY amount DESC LIMIT ").push_bind(limit);

    match qb.build_query_as::<TransactionRow>().fetch_all(db.get_ref()).await {
        Ok(rows) => HttpResponse::Ok().json(serde_json::json!({ "status": "success", "data": rows })),
        Err(e) => {
            log::error!("[top_transactions] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/transactions/stats/channel — internal vs external totals
pub async fn channel_stats(admin: AdminAuth, db: web::Data<PgPool>) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let rows = sqlx::query!(
        r#"SELECT channel, COUNT(*) as "count!", COALESCE(SUM(amount), 0) as "total!"
           FROM transactions
           WHERE status = 'success'
           GROUP BY channel"#
    )
    .fetch_all(db.get_ref())
    .await;

    match rows {
        Ok(rows) => {
            let data: Vec<_> = rows.iter().map(|r| serde_json::json!({
                "channel": r.channel, "count": r.count, "total": r.total
            })).collect();
            HttpResponse::Ok().json(serde_json::json!({ "status": "success", "data": data }))
        }
        Err(e) => {
            log::error!("[channel_stats] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/transactions/stats/volume — daily / last week / 2 weeks / month comparison
pub async fn volume_stats(admin: AdminAuth, db: web::Data<PgPool>) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    // Fixed, allowlisted intervals — safe to interpolate since none of this
    // comes from user input.
    let periods = [("today", "1 day"), ("last_week", "7 days"), ("2weeks", "14 days"), ("month", "30 days")];

    let mut results = Vec::with_capacity(periods.len());
    for (label, interval) in periods {
        let sql = format!(
            "SELECT COUNT(*) as count, COALESCE(SUM(amount), 0) as total \
             FROM transactions WHERE status = 'success' AND created_at >= NOW() - INTERVAL '{}'",
            interval
        );
        match sqlx::query(&sql).fetch_one(db.get_ref()).await {
            Ok(row) => {
                use sqlx::Row;
                let count: i64 = row.try_get("count").unwrap_or(0);
                let total: BigDecimal = row.try_get("total").unwrap_or_default();
                results.push(serde_json::json!({ "period": label, "count": count, "total": total }));
            }
            Err(e) => {
                log::error!("[volume_stats] DB error for {}: {}", label, e);
                return HttpResponse::InternalServerError().finish();
            }
        }
    }

    let highest = results.iter().max_by(|a, b| {
        a["total"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0)
            .partial_cmp(&b["total"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0))
            .unwrap()
    }).cloned();

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success", "data": results, "highest_period": highest
    }))
}

/// GET /admin/transactions/search — between two accounts, on a date, capped amount, status
pub async fn search_transactions_between_accounts(
    admin: AdminAuth,
    query: web::Query<SearchBetweenAccountsQuery>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }
    if let Err(errors) = query.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error", "message": "Invalid input", "errors": errors
        }));
    }

    let account_a = sqlx::query!(
        "SELECT id, account_number FROM accounts WHERE account_number = $1",
        query.account_a
    ).fetch_optional(db.get_ref()).await;
    let account_b = sqlx::query!(
        "SELECT id, account_number FROM accounts WHERE account_number = $1",
        query.account_b
    ).fetch_optional(db.get_ref()).await;

    let (a, b) = match (account_a, account_b) {
        (Ok(Some(a)), Ok(Some(b))) => (a, b),
        (Ok(None), _) | (_, Ok(None)) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "One or both account numbers were not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {
            return HttpResponse::InternalServerError().finish();
        }
    };

    let date = match &query.date {
        Some(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
            Ok(d) => Some(d),
            Err(_) => return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: "date must be in YYYY-MM-DD format".into(),
                status: ResponseStatus::ERROR,
            }),
        },
        None => None,
    };

    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "SELECT {} FROM transactions WHERE ((sender_id = ", TX_COLUMNS
    ));
    qb.push_bind(a.id);
    qb.push(" AND (reciever_id = ").push_bind(b.id);
    qb.push(" OR reciever_account_number = ").push_bind(b.account_number.clone());
    qb.push(")) OR (sender_id = ").push_bind(b.id);
    qb.push(" AND (reciever_id = ").push_bind(a.id);
    qb.push(" OR reciever_account_number = ").push_bind(a.account_number.clone());
    qb.push(")))");

    if let Some(date) = date {
        qb.push(" AND created_at::date = ").push_bind(date);
    }
    if let Some(max_amount) = &query.max_amount {
        qb.push(" AND amount <= ").push_bind(max_amount);
    }
    // Default to success if not specified — matches "where status is
    // success or so" reading of the request.
    qb.push(" AND status = ").push_bind(query.status.clone().unwrap_or_else(|| "success".into()));

    qb.push(" ORDER BY created_at DESC");

    match qb.build_query_as::<TransactionRow>().fetch_all(db.get_ref()).await {
        Ok(rows) => HttpResponse::Ok().json(serde_json::json!({ "status": "success", "data": rows })),
        Err(e) => {
            log::error!("[search_transactions_between_accounts] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
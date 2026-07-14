use actix_web::{web, HttpResponse, Responder};
use sqlx::{PgPool, Postgres, QueryBuilder};
use validator::Validate;
use crate::middlewares::admin_auth::{AdminAuth, require_role};
use crate::modules::admin::vas::schemas::ListVasQuery;
use crate::utils::responder::{ApiResponse, ResponseStatus};

const READ_ROLES: &[&str] = &["admin", "support", "compliance"];

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct VasTransactionRow {
    pub id: uuid::Uuid,
    pub account_id: uuid::Uuid,
    pub vas_type: String,
    pub network: Option<String>,
    pub provider: Option<String>,
    pub recipient: String,
    pub amount: bigdecimal::BigDecimal,
    pub currency: String,
    pub reference: String,
    pub debit_account: String,
    pub status: String,
    pub response_code: Option<String>,
    pub response_message: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

const VAS_COLUMNS: &str = "id, account_id, vas_type, network, provider, recipient, \
    amount, currency, reference, debit_account, status, response_code, response_message, \
    created_at, updated_at";

/// GET /admin/vas-transactions
pub async fn list_vas_transactions(
    admin: AdminAuth,
    query: web::Query<ListVasQuery>,
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
        QueryBuilder::new(format!("SELECT {} FROM vas_transactions WHERE 1=1", VAS_COLUMNS));

    if let Some(vas_type) = &query.vas_type {
        qb.push(" AND vas_type = ").push_bind(vas_type);
    }
    if let Some(network) = &query.network {
        qb.push(" AND network = ").push_bind(network);
    }
    if let Some(status) = &query.status {
        qb.push(" AND status = ").push_bind(status);
    }

    qb.push(format!(" ORDER BY {} {}", sort_col, order));
    qb.push(" LIMIT ").push_bind(limit);
    qb.push(" OFFSET ").push_bind(offset);

    match qb.build_query_as::<VasTransactionRow>().fetch_all(db.get_ref()).await {
        Ok(rows) => HttpResponse::Ok().json(serde_json::json!({
            "status": "success", "page": page, "limit": limit, "data": rows
        })),
        Err(e) => {
            log::error!("[list_vas_transactions] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/vas-transactions/stats — totals overall, per user, and highest user
pub async fn vas_stats(admin: AdminAuth, db: web::Data<PgPool>) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let overall = sqlx::query!(
        r#"SELECT COUNT(*) as "count!", COALESCE(SUM(amount), 0) as "total"
           FROM vas_transactions WHERE status = 'success'"#
    )
    .fetch_one(db.get_ref())
    .await;

    let by_user = sqlx::query!(
        r#"SELECT vt.account_id, a.email, a.firstname, a.lastname,
               COUNT(*) as "count!", COALESCE(SUM(vt.amount), 0) as "total"
           FROM vas_transactions vt
           JOIN accounts a ON a.id = vt.account_id
           WHERE vt.status = 'success'
           GROUP BY vt.account_id, a.email, a.firstname, a.lastname
           ORDER BY total DESC
           LIMIT 20"#
    )
    .fetch_all(db.get_ref())
    .await;

    match (overall, by_user) {
        (Ok(overall), Ok(by_user)) => {
            let by_user_json: Vec<_> = by_user.iter().map(|r| serde_json::json!({
                "account_id": r.account_id,
                "email": r.email,
                "firstname": r.firstname,
                "lastname": r.lastname,
                "count": r.count,
                "total": r.total,
            })).collect();

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data": {
                    "total_count": overall.count,
                    "total_amount": overall.total,
                    "by_user": by_user_json,
                    "highest_user": by_user_json.first(),
                }
            }))
        }
        _ => HttpResponse::InternalServerError().finish(),
    }
}
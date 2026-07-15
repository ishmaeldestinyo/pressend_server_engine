use crate::config::Config;
use crate::middlewares::admin_auth::issue_admin_token;
use crate::middlewares::admin_auth::{AdminAuth, require_role};
use crate::modules::admin::schemas::LoginRequest;
use crate::modules::admin::schemas::{ListAccountsQuery, ReviewAccountBody};
use crate::utils::password_manager::verify_password;
use crate::utils::psb::PsbClient;
use crate::utils::responder::{ApiResponse, ResponseStatus};
use actix_web::{HttpResponse, Responder, web};
use sqlx::PgPool;
use validator::Validate;

const SUSPEND_ROLES: &[&str] = &["admin", "compliance"];

const READ_ROLES: &[&str] = &["admin", "support", "compliance"];

const ADMIN_ROLES: &[(&str, &str)] = &[
    (
        "super_admin",
        "Full access to all admin operations, including managing other admins",
    ),
    (
        "admin",
        "General admin access — accounts, wallets, support actions",
    ),
    (
        "compliance",
        "KYC review, account suspension, and compliance-related actions",
    ),
    (
        "support",
        "Read-only access to account details for customer support",
    ),
];

/// GET/POST /admin/users — paginated, filterable summary list
pub async fn get_accounts(
    admin: AdminAuth,
    query: web::Query<ListAccountsQuery>,
    db: web::Data<sqlx::PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    if let Err(errors) = query.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error",
            "message": "Invalid input",
            "errors": errors
        }));
    }

    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * limit;

    let q = query.q.as_deref().map(|s| s.trim().to_lowercase());
    let like_q = q.as_ref().map(|s| format!("%{}%", s));

    let rows = sqlx::query!(
        r#"
        SELECT
            id, email, firstname, lastname, othername, phone_number,
            account_number, account_name, account_type, status, current_tier,
            email_verified, phone_no_verified, created_at, updated_at
        FROM accounts
        WHERE deleted_at IS NULL
          AND ($1::text IS NULL OR status = $1)
          AND ($2::text IS NULL OR account_type = $2)
          AND ($3::smallint IS NULL OR current_tier = $3)
          AND (
              $4::text IS NULL
              OR LOWER(email) LIKE $4
              OR LOWER(firstname) LIKE $4
              OR LOWER(lastname) LIKE $4
              OR TRIM(account_number::text) = $5
              OR TRIM(phone_number) = $5
          )
        ORDER BY created_at DESC
        LIMIT $6 OFFSET $7
        "#,
        query.status,
        query.account_type,
        query.current_tier,
        like_q,
        q,
        limit,
        offset
    )
    .fetch_all(db.get_ref())
    .await;

    match rows {
        Ok(rows) => {
            let data: Vec<_> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "email": r.email,
                        "firstname": r.firstname,
                        "lastname": r.lastname,
                        "othername": r.othername,
                        "phone_number": r.phone_number,
                        "account_number": r.account_number,
                        "account_name": r.account_name,
                        "account_type": r.account_type,
                        "status": r.status,
                        "current_tier": r.current_tier,
                        "email_verified": r.email_verified,
                        "phone_no_verified": r.phone_no_verified,
                        "created_at": r.created_at,
                        "updated_at": r.updated_at,
                    })
                })
                .collect();

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "page": page,
                "limit": limit,
                "data": data
            }))
        }
        Err(e) => {
            log::error!("[get_accounts] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/users/{id} — full record, mirrors get_user_info but for any account
pub async fn get_account_details(
    admin: AdminAuth,
    path: web::Path<String>,
    db: web::Data<sqlx::PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let account_uuid = match uuid::Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account id".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let (account, contact_history): (Result<_, sqlx::Error>, Result<_, sqlx::Error>) = tokio::join!(
        sqlx::query!(
            r#"SELECT id, email, firstname, lastname, othername, phone_number,
               email_verified, phone_no_verified, status, account_type,
               is_2fa_enabled, user_photo, device_id, current_tier, pending_tier_upgrade,
               tier_upgraded_at, tier_upgrade_requested_at, panic_enabled,
               panic_message, panic_activated_at, panic_deactivated_at,
               account_number, account_name, bvn, nin,
               nin_userid, created_at, updated_at, referral_code
               FROM accounts WHERE id = $1 AND deleted_at IS NULL"#,
            account_uuid
        )
        .fetch_one(db.get_ref()),
        sqlx::query!(
            r#"SELECT id, field, old_value, new_value, registered_at, changed_at
               FROM account_contact_history
               WHERE account_id = $1
               ORDER BY registered_at DESC"#,
            account_uuid
        )
        .fetch_all(db.get_ref())
    );

    let row = match account {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let history = contact_history
        .unwrap_or_default()
        .iter()
        .map(|h| {
            serde_json::json!({
                "id": h.id,
                "field": h.field,
                "old_value": h.old_value,
                "new_value": h.new_value,
                "registered_at": h.registered_at,
                "changed_at": h.changed_at,
            })
        })
        .collect::<Vec<_>>();

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": {
            "id": row.id,
            "email": row.email,
            "firstname": row.firstname,
            "lastname": row.lastname,
            "othername": row.othername,
            "phone_number": row.phone_number,
            "email_verified": row.email_verified,
            "phone_no_verified": row.phone_no_verified,
            "status": row.status,
            "account_type": row.account_type,
            "is_2fa_enabled": row.is_2fa_enabled,
            "user_photo": row.user_photo,
            "device_id": row.device_id,
            "current_tier": row.current_tier,
            "pending_tier_upgrade": row.pending_tier_upgrade,
            "tier_upgraded_at": row.tier_upgraded_at,
            "tier_upgrade_requested_at": row.tier_upgrade_requested_at,
            "panic_enabled": row.panic_enabled,
            "panic_message": row.panic_message,
            "panic_activated_at": row.panic_activated_at,
            "panic_deactivated_at": row.panic_deactivated_at,
            "account_number": row.account_number,
            "account_name": row.account_name,
            "bvn": row.bvn,
            "nin": row.nin,
            "nin_userid": row.nin_userid,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "referral_code": row.referral_code,
            "contact_history": history,
        }
    }))
}

/// GET /admin/users/{id}/wallet — PSB wallet lookup for a specific account
pub async fn get_account_wallet(
    admin: AdminAuth,
    path: web::Path<String>,
    db: web::Data<sqlx::PgPool>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, READ_ROLES) {
        return resp;
    }

    let account_uuid = match uuid::Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account id".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let row = sqlx::query!(
        "SELECT id, account_number, current_tier FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_one(db.get_ref())
    .await;

    let row = match row {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let account_number = match row.account_number {
        Some(n) if !n.is_empty() => n,
        _ => {
            return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: "Account has no linked wallet".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let mut redis_conn = redis.get_ref().clone();

    let wallet = PsbClient::new(&cfg)
        .post(
            &mut redis_conn,
            "/waas/api/v1/wallet_enquiry",
            &serde_json::json!({ "accountNo": account_number }),
        )
        .await;

    match wallet {
        Ok(response) => {
            let status = response["status"].as_str().unwrap_or("").to_uppercase();
            if status != "SUCCESS" {
                return HttpResponse::BadGateway().json(ApiResponse {
                    message: "Wallet lookup failed".into(),
                    status: ResponseStatus::ERROR,
                });
            }

            let today = chrono::Utc::now().format("%Y-%m-%d");
            let daily_key = format!("daily_txn_total:{}:{}", row.id, today);
            let daily_total: f64 = redis::cmd("GET")
                .arg(&daily_key)
                .query_async(&mut redis_conn)
                .await
                .unwrap_or(None)
                .unwrap_or(0.0);

            let daily_limit: Option<f64> = match row.current_tier {
                1 => Some(50_000.0),
                2 => Some(200_000.0),
                _ => None,
            };
            let exceeded_limit = daily_limit.map(|limit| daily_total >= limit);

            let mut wallet_data = response["data"].clone();
            wallet_data["daily_outgoing_total"] = serde_json::json!(daily_total);
            wallet_data["daily_limit"] = serde_json::json!(daily_limit);
            wallet_data["exceeded_limit"] = serde_json::json!(exceeded_limit);

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "data": wallet_data
            }))
        }
        Err(e) => {
            log::warn!(
                "[get_account_wallet] PSB wallet_enquiry failed for {}: {}",
                account_number,
                e
            );
            HttpResponse::BadGateway().json(ApiResponse {
                message: "Wallet lookup failed".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

const REVIEW_ROLES: &[&str] = SUSPEND_ROLES; // same roles as before, renamed for clarity if you'd rather not share the const

pub async fn review_account(
    admin: AdminAuth,
    path: web::Path<String>,
    body: web::Json<ReviewAccountBody>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, SUSPEND_ROLES) {
        return resp;
    }

    if !matches!(body.status.as_str(), "active" | "suspended" | "disabled") {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: "status must be 'active', 'suspended', or 'disabled'".into(),
            status: ResponseStatus::ERROR,
        });
    }

    if let Err(errors) = body.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error",
            "message": "Invalid input",
            "errors": errors
        }));
    }

    let account_uuid = match uuid::Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account id".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let row = sqlx::query!(
        r#"SELECT id, status FROM accounts WHERE id = $1 AND deleted_at IS NULL"#,
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    let row = match row {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[review_account] DB error fetching account: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    if row.status == "deleted" {
        return HttpResponse::Conflict().json(ApiResponse {
            message: "Cannot review a deleted account".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let status_changed = row.status != body.status;

    let updated = sqlx::query!(
        r#"UPDATE accounts
           SET status = $1,
               admin_note = COALESCE($2, admin_note),
               admin_note_updated_at = CASE WHEN $2 IS NOT NULL THEN NOW() ELSE admin_note_updated_at END,
               updated_at = NOW()
           WHERE id = $3"#,
        body.status,
        body.note.as_deref(),
        account_uuid
    )
    .execute(db.get_ref())
    .await;

    if let Err(e) = updated {
        log::error!("[review_account] failed to update account: {}", e);
        return HttpResponse::InternalServerError().finish();
    }

    if status_changed {
        let mut redis_conn = redis.get_ref().clone();
        let cache_key = format!("account:{}", account_uuid);
        let _: Result<(), redis::RedisError> = redis::cmd("DEL")
            .arg(&cache_key)
            .query_async(&mut redis_conn)
            .await;
    }

    log::info!(
        "[review_account] admin {} set account {} to '{}' (reason: {:?}, note: {})",
        admin.id,
        account_uuid,
        body.status,
        body.reason,
        body.note.is_some()
    );

    HttpResponse::Ok().json(ApiResponse {
        message: "Account reviewed successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}


/// POST /admin/login
pub async fn login(
    req: actix_web::HttpRequest,
    body: web::Json<LoginRequest>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(errors) = body.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error", "message": "Invalid input", "errors": errors
        }));
    }

    let row = sqlx::query!(
        r#"SELECT id, password_hash, role, status
           FROM admins
           WHERE email = $1 AND deleted_at IS NULL"#,
        body.email.to_lowercase()
    )
    .fetch_optional(db.get_ref())
    .await;

    let row = match row {
        Ok(Some(r)) => r,
        Ok(None) => {
            // Same response as a bad password — don't reveal whether the email exists.
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid email or password".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[login] DB error: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let is_valid = match verify_password(&body.password, &row.password_hash) {
        Ok(valid) => valid,
        Err(e) => {
            log::error!("[login] password verify error: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    if !is_valid {
        return HttpResponse::Unauthorized().json(ApiResponse {
            message: "Invalid email or password".into(),
            status: ResponseStatus::ERROR,
        });
    }

    if row.status != "active" {
        return HttpResponse::Forbidden().json(ApiResponse {
            message: "Admin account is not active".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let token = match issue_admin_token(row.id) {
        Ok(t) => t,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let ip = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();

    if let Err(e) = sqlx::query!(
        r#"UPDATE admins SET last_login_at = NOW(), last_login_ip = $1 WHERE id = $2"#,
        ip,
        row.id
    )
    .execute(db.get_ref())
    .await
    {
        // Non-fatal — log but don't fail the login over a metadata write.
        log::warn!("[login] failed to update last_login metadata: {}", e);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": { "token": token, "role": row.role }
    }))
}

/// GET /admin/roles — list valid admin roles
pub async fn get_roles(admin: AdminAuth) -> impl Responder {
    if let Err(resp) = require_role(&admin, &[]) {
        // require_role always allows super_admin regardless of the `allowed`
        // list, so passing &[] here means: super_admin only.
        return resp;
    }

    let roles: Vec<_> = ADMIN_ROLES
        .iter()
        .map(|(role, description)| {
            serde_json::json!({
                "role": role,
                "description": description,
            })
        })
        .collect();

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": roles
    }))
}

use actix_web::{HttpResponse, Responder, web};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    modules::beneficiary::schemas,
    utils::{
        jwt::AuthUser,
        responder::{ApiResponse, ResponseStatus, ValidationErrorResponse},
    },
};

const TTL_7_DAYS: u64 = 604_800;

fn cache_key(account_id: &str) -> String {
    format!("beneficiaries:{}", account_id)
}

// ── Add beneficiary ───────────────────────────────────────────────────────────

pub async fn add_beneficiary(
    auth: AuthUser,
    body: web::Json<schemas::AddBeneficiaryRequest>,
    db: web::Data<PgPool>,
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

    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Validate product_type ─────────────────────────────────────────────────
    if !["bank", "vas"].contains(&body.product_type.as_str()) {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Invalid product type. Must be either 'bank' or 'vas'.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Insert ────────────────────────────────────────────────────────────────
    let result = sqlx::query!(
        r#"
    INSERT INTO beneficiaries
        (account_id, product_type, recipient, service_name, service_code, label)
    VALUES
        ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (account_id, recipient, service_code)
    WHERE deleted_at IS NULL
    DO NOTHING
    RETURNING id
    "#,
        account_uuid,
        body.product_type,
        body.recipient,
        body.service_name,
        body.service_code,
        body.label,
    )
    .fetch_optional(db.get_ref())
    .await;

    match result {
        Ok(Some(_)) => {
            // ── Bust cache ────────────────────────────────────────────────────
            let mut redis_conn = redis.get_ref().clone();
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(&cache_key(&auth.id))
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Created().json(ApiResponse {
                message: "Beneficiary added successfully.".into(),
                status: ResponseStatus::SUCCESS,
            })
        }
        Ok(None) => HttpResponse::Conflict().json(ApiResponse {
            message: "This beneficiary already exists in your list.".into(),
            status: ResponseStatus::ERROR,
        }),
        Err(e) => {
            println!("[add_beneficiary] DB error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

// ── List beneficiaries ────────────────────────────────────────────────────────

pub async fn list_beneficiaries(
    auth: AuthUser,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let mut redis_conn = redis.get_ref().clone();
    let key = cache_key(&auth.id);

    // ── Check cache ───────────────────────────────────────────────────────────
    let cached: Option<String> = match redis::cmd("GET")
        .arg(&key)
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

    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch from DB ─────────────────────────────────────────────────────────
    let rows = sqlx::query!(
        r#"
        SELECT id, product_type, recipient, service_name, service_code, label, created_at
        FROM beneficiaries
        WHERE account_id = $1 AND deleted_at IS NULL
        ORDER BY created_at DESC
        "#,
        account_uuid
    )
    .fetch_all(db.get_ref())
    .await;

    let beneficiaries = match rows {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "id":           r.id,
                    "product_type": r.product_type,
                    "recipient":    r.recipient,
                    "service_name": r.service_name,
                    "service_code": r.service_code,
                    "label":        r.label,
                    "created_at":   r.created_at,
                })
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            println!("[list_beneficiaries] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let data = serde_json::json!({ "beneficiaries": beneficiaries });

    // ── Cache for 7 days ──────────────────────────────────────────────────────
    let _: Result<(), _> = redis::cmd("SETEX")
        .arg(&key)
        .arg(TTL_7_DAYS)
        .arg(data.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data":   data
    }))
}

// ── Update beneficiary ────────────────────────────────────────────────────────

pub async fn update_beneficiary(
    auth: AuthUser,
    path: web::Path<Uuid>,
    body: web::Json<schemas::UpdateBeneficiaryRequest>,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
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

    let beneficiary_id = path.into_inner();

    let result = sqlx::query!(
        r#"
        UPDATE beneficiaries
        SET
            label        = COALESCE($1, label),
            service_name = COALESCE($2, service_name),
            updated_at   = NOW()
        WHERE id = $3
          AND account_id = $4
          AND deleted_at IS NULL
        "#,
        body.label,
        body.service_name,
        beneficiary_id,
        account_uuid,
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => HttpResponse::NotFound().json(ApiResponse {
            message: "Beneficiary not found.".into(),
            status: ResponseStatus::ERROR,
        }),
        Ok(_) => {
            // ── Bust cache ────────────────────────────────────────────────────
            let mut redis_conn = redis.get_ref().clone();
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(&cache_key(&auth.id))
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(ApiResponse {
                message: "Beneficiary updated successfully.".into(),
                status: ResponseStatus::SUCCESS,
            })
        }
        Err(e) => {
            println!("[update_beneficiary] DB error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

// ── Delete beneficiary (soft delete) ─────────────────────────────────────────

pub async fn delete_beneficiary(
    auth: AuthUser,
    path: web::Path<Uuid>,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
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

    let beneficiary_id = path.into_inner();

    let result = sqlx::query!(
        r#"
        UPDATE beneficiaries
        SET deleted_at = NOW(), updated_at = NOW()
        WHERE id = $1
          AND account_id = $2
          AND deleted_at IS NULL
        "#,
        beneficiary_id,
        account_uuid,
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => HttpResponse::NotFound().json(ApiResponse {
            message: "Beneficiary not found.".into(),
            status: ResponseStatus::ERROR,
        }),
        Ok(_) => {
            // ── Bust cache ────────────────────────────────────────────────────
            let mut redis_conn = redis.get_ref().clone();
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(&cache_key(&auth.id))
                .query_async(&mut redis_conn)
                .await;

            HttpResponse::Ok().json(ApiResponse {
                message: "Beneficiary removed successfully.".into(),
                status: ResponseStatus::SUCCESS,
            })
        }
        Err(e) => {
            println!("[delete_beneficiary] DB error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

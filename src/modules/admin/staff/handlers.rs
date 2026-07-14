use actix_web::{web, HttpResponse, Responder};
use sqlx::PgPool;
use validator::Validate;
use crate::middlewares::admin_auth::{AdminAuth, require_role};
use crate::modules::admin::staff::schemas::{CreateStaffRequest, ResetPasswordRequest, ListStaffQuery};
use crate::utils::password_manager::hash_password;
use crate::utils::responder::{ApiResponse, ResponseStatus};

/// POST /admin/staff — super_admin only.
pub async fn create_staff(
    admin: AdminAuth,
    body: web::Json<CreateStaffRequest>,
    db: web::Data<PgPool>,
) -> impl Responder {
    // Empty allowlist — only the super_admin short-circuit in require_role
    // lets anyone through this check.
    if let Err(resp) = require_role(&admin, &[]) {
        return resp;
    }
    if let Err(errors) = body.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error", "message": "Invalid input", "errors": errors
        }));
    }

    let password_hash = match hash_password(&body.password) {
        Ok(h) => h,
        Err(e) => {
            log::error!("[create_staff] password hash error: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let admin_id = match uuid::Uuid::parse_str(&admin.id) {
        Ok(id) => id,
        Err(_) => return HttpResponse::InternalServerError().finish(),
    };

    let result = sqlx::query!(
        r#"INSERT INTO admins (email, password_hash, firstname, lastname, role, invited_by)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id, email, firstname, lastname, role, status, created_at"#,
        body.email.to_lowercase(),
        password_hash,
        body.firstname,
        body.lastname,
        body.role,
        admin_id
    )
    .fetch_one(db.get_ref())
    .await;

    match result {
        Ok(row) => HttpResponse::Created().json(serde_json::json!({
            "status": "success",
            "data": {
                "id": row.id, "email": row.email, "firstname": row.firstname,
                "lastname": row.lastname, "role": row.role, "status": row.status,
                "created_at": row.created_at,
            }
        })),
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            HttpResponse::Conflict().json(ApiResponse {
                message: "An admin with this email already exists".into(),
                status: ResponseStatus::ERROR,
            })
        }
        Err(e) => {
            log::error!("[create_staff] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// PATCH /admin/staff/{id}/reset-password — admin or super_admin.
pub async fn reset_staff_password(
    admin: AdminAuth,
    path: web::Path<String>,
    body: web::Json<ResetPasswordRequest>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, &["admin"]) {
        return resp;
    }
    if let Err(errors) = body.validate() {
        return HttpResponse::UnprocessableEntity().json(serde_json::json!({
            "status": "error", "message": "Invalid input", "errors": errors
        }));
    }

    let staff_id = match uuid::Uuid::parse_str(&path.into_inner()) {
        Ok(id) => id,
        Err(_) => return HttpResponse::BadRequest().json(ApiResponse {
            message: "Invalid staff id".into(),
            status: ResponseStatus::ERROR,
        }),
    };

    let target = sqlx::query!(
        r#"SELECT role FROM admins WHERE id = $1 AND deleted_at IS NULL"#,
        staff_id
    )
    .fetch_optional(db.get_ref())
    .await;

    let target_role = match target {
        Ok(Some(row)) => row.role,
        Ok(None) => return HttpResponse::NotFound().json(ApiResponse {
            message: "Staff member not found".into(),
            status: ResponseStatus::ERROR,
        }),
        Err(e) => {
            log::error!("[reset_staff_password] DB error (lookup): {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    // A plain "admin" cannot reset a super_admin's password — only another super_admin can.
    if target_role == "super_admin" && admin.role != "super_admin" {
        return HttpResponse::Forbidden().json(ApiResponse {
            message: "Only a super_admin can reset a super_admin's password".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let password_hash = match hash_password(&body.new_password) {
        Ok(h) => h,
        Err(e) => {
            log::error!("[reset_staff_password] password hash error: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let result = sqlx::query!(
        r#"UPDATE admins SET password_hash = $1, updated_at = NOW() WHERE id = $2"#,
        password_hash,
        staff_id
    )
    .execute(db.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() > 0 => HttpResponse::Ok().json(ApiResponse {
            message: "Password reset successfully".into(),
            status: ResponseStatus::SUCCESS,
        }),
        Ok(_) => HttpResponse::NotFound().json(ApiResponse {
            message: "Staff member not found".into(),
            status: ResponseStatus::ERROR,
        }),
        Err(e) => {
            log::error!("[reset_staff_password] DB error (update): {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// GET /admin/staff — list staff (never returns password_hash).
pub async fn list_staff(
    admin: AdminAuth,
    query: web::Query<ListStaffQuery>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(resp) = require_role(&admin, &["admin"]) {
        return resp;
    }

    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * limit;

    let rows = sqlx::query!(
        r#"SELECT id, email, firstname, lastname, role, status, last_login_at, created_at
           FROM admins
           WHERE deleted_at IS NULL
             AND ($1::text IS NULL OR role = $1)
             AND ($2::text IS NULL OR status = $2)
           ORDER BY created_at DESC
           LIMIT $3 OFFSET $4"#,
        query.role,
        query.status,
        limit,
        offset
    )
    .fetch_all(db.get_ref())
    .await;

    match rows {
        Ok(rows) => {
            let data: Vec<_> = rows.iter().map(|r| serde_json::json!({
                "id": r.id, "email": r.email, "firstname": r.firstname,
                "lastname": r.lastname, "role": r.role, "status": r.status,
                "last_login_at": r.last_login_at, "created_at": r.created_at,
            })).collect();
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success", "page": page, "limit": limit, "data": data
            }))
        }
        Err(e) => {
            log::error!("[list_staff] DB error: {}", e);
            HttpResponse::InternalServerError().finish()
        }
    }
}
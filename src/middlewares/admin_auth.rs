use actix_web::{dev::Payload, error, web, Error, FromRequest, HttpRequest};
use futures_util::future::LocalBoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation, Algorithm};
use chrono::{Duration, Utc};

#[derive(Debug, Serialize, Deserialize)]
struct AdminClaims {
    sub: String,   // admin id (uuid)
    exp: usize,
}

#[derive(Debug, Clone)]
pub struct AdminAuth {
    pub id: String,
    pub role: String,
    pub permissions: Vec<String>,
}

impl FromRequest for AdminAuth {
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self, Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let req = req.clone();

        Box::pin(async move {
            // 1. Pull bearer token
            let auth_header = req
                .headers()
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| error::ErrorUnauthorized("Missing authorization header"))?;

            let token = auth_header
                .strip_prefix("Bearer ")
                .ok_or_else(|| error::ErrorUnauthorized("Invalid authorization scheme"))?;

            // 2. Decode + verify signature/expiry
            // NOTE: use a dedicated admin secret, distinct from the user-facing
            // one, so a leaked user JWT secret can't be used to mint admin tokens.
            let secret = env::var("ADMIN_JWT_SECRET")
                .map_err(|_| error::ErrorInternalServerError("Admin JWT secret not configured"))?;

            let token_data = decode::<AdminClaims>(
                token,
                &DecodingKey::from_secret(secret.as_bytes()),
                &Validation::new(Algorithm::HS256),
            )
            .map_err(|_| error::ErrorUnauthorized("Invalid or expired token"))?;

            let admin_id = uuid::Uuid::parse_str(&token_data.claims.sub)
                .map_err(|_| error::ErrorUnauthorized("Invalid token subject"))?;

            // 3. Re-check role/status against the DB — don't trust a stale JWT claim.
            let pool = req
                .app_data::<web::Data<PgPool>>()
                .ok_or_else(|| error::ErrorInternalServerError("DB pool not available"))?;

            let row = sqlx::query!(
                r#"SELECT id, role, permissions, status
                   FROM admins
                   WHERE id = $1 AND deleted_at IS NULL"#,
                admin_id
            )
            .fetch_optional(pool.get_ref())
            .await
            .map_err(|_| error::ErrorInternalServerError("Failed to verify admin"))?;

            let row = row.ok_or_else(|| error::ErrorUnauthorized("Admin not found"))?;

            if row.status != "active" {
                return Err(error::ErrorForbidden("Admin account is not active"));
            }

            let permissions: Vec<String> = serde_json::from_value(row.permissions).unwrap_or_default();

            Ok(AdminAuth {
                id: row.id.to_string(),
                role: row.role,
                permissions,
            })
        })
    }
}

/// Call at the top of every admin handler, right after extraction.
pub fn require_role(admin: &AdminAuth, allowed: &[&str]) -> Result<(), actix_web::HttpResponse> {
    if admin.role == "super_admin" || allowed.contains(&admin.role.as_str()) {
        Ok(())
    } else {
        Err(actix_web::HttpResponse::Forbidden().json(serde_json::json!({
            "status": "error",
            "message": "Insufficient permissions"
        })))
    }
}

/// Issue a signed admin JWT after successful login. Uses the same
/// ADMIN_JWT_SECRET as verification, and a 12-hour expiry.
pub fn issue_admin_token(admin_id: uuid::Uuid) -> Result<String, Error> {
    let secret = env::var("ADMIN_JWT_SECRET")
        .map_err(|_| error::ErrorInternalServerError("Admin JWT secret not configured"))?;

    let claims = AdminClaims {
        sub: admin_id.to_string(),
        exp: (Utc::now() + Duration::hours(12)).timestamp() as usize,
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|_| error::ErrorInternalServerError("Failed to issue token"))
}
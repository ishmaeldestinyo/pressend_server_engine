use actix_web::dev::Payload;
use actix_web::{web, FromRequest, HttpRequest};
use std::future::{ready, Ready};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use chrono::Utc;

#[derive(Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,        // account id
    pub exp: usize,         // expiry
    pub iat: usize,         // issued at
    pub token_type: String, // "access" or "refresh"
}

// ── Auth extractor ────────────────────────────────────────────────────────────

pub struct AuthUser {
    pub id: String,
    pub token: Option<String>, 
}

impl FromRequest for AuthUser {
    type Error = actix_web::Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let secret = match req.app_data::<web::Data<crate::config::Config>>() {
            Some(cfg) => cfg.jwt_secret.clone(),
            None => {
                return ready(Err(actix_web::error::ErrorInternalServerError(
                    "Config unavailable",
                )));
            }
        };

        let token = match req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        {
            Some(t) => t.to_string(),
            None => {
                return ready(Err(actix_web::error::ErrorUnauthorized(
                    "Missing or invalid Authorization header",
                )));
            }
        };

        match verify_token(&token, &secret) {
            Ok(claims) if claims.token_type == "access" => {
                ready(Ok(AuthUser { id: claims.sub, token: Some(token) })) 
            }
            Ok(_) => ready(Err(actix_web::error::ErrorUnauthorized(
                "Invalid token type",
            ))),
            Err(_) => ready(Err(actix_web::error::ErrorUnauthorized(
                "Invalid or expired token",
            ))),
        }
    }
}// ── Token generation ──────────────────────────────────────────────────────────

pub fn generate_access_token(id: &str, secret: &str) -> Result<String, String> {
    let now = Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: id.to_string(),
        exp: now + 30 * 60, // 30 minutes
        iat: now,
        token_type: "access".into(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("Token error: {}", e))
}

pub fn generate_refresh_token(id: &str, secret: &str) -> Result<String, String> {
    let now = Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: id.to_string(),
        exp: now + 7 * 24 * 60 * 60, // 7 days
        iat: now,
        token_type: "refresh".into(),
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| format!("Token error: {}", e))
}

pub fn verify_token(token: &str, secret: &str) -> Result<Claims, String> {
    decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .map(|data| data.claims)
    .map_err(|e| format!("Invalid token: {}", e))
}
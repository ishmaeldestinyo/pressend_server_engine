use actix_governor::governor::clock::{Clock, DefaultClock, QuantaInstant};
use actix_governor::governor::NotUntil;
use actix_governor::{KeyExtractor, SimpleKeyExtractionError};
use actix_web::dev::ServiceRequest;
use actix_web::{web, HttpResponse, HttpResponseBuilder};
use serde_json::json;
use crate::utils::jwt::verify_token;

#[derive(Clone)]
pub struct UserAndIpKey;

impl KeyExtractor for UserAndIpKey {
    type Key = String;
    type KeyExtractionError = SimpleKeyExtractionError<&'static str>;

    fn extract(&self, req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError> {
        // ── 1. Resolve IP ─────────────────────────────────────────────────
        let ip = req
            .connection_info()
            .realip_remote_addr()
            .unwrap_or("unknown")
            .to_string();

        // ── 2. Resolve user id from JWT ───────────────────────────────────
        let user_id = (|| -> Option<String> {
            let secret = req
                .app_data::<web::Data<crate::config::Config>>()?
                .jwt_secret
                .clone();

            let token = req
                .headers()
                .get("Authorization")?
                .to_str()
                .ok()?
                .strip_prefix("Bearer ")?
                .to_string();

            let claims = verify_token(&token, &secret).ok()?;

            if claims.token_type == "access" {
                Some(claims.sub)
            } else {
                None
            }
        })()
        .unwrap_or_else(|| ip.clone());

        // ── 3. Composite key ──────────────────────────────────────────────
        let key = format!("{}:{}", user_id, ip);
        // log::info!("🔑 Rate limit key: {}", key); // ← debug log
        Ok(key)
    }

    fn exceed_rate_limit_response(
        &self,
        negative: &NotUntil<QuantaInstant>,
        mut response: HttpResponseBuilder,
    ) -> HttpResponse {
        let wait_secs = negative
            .wait_time_from(DefaultClock::default().now())
            .as_secs();

        response.json(json!({
            "status":  "error",
            "message": format!("Too many requests. Please retry after {} seconds.", wait_secs)
        }))
    }
}
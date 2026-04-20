use actix_governor::{KeyExtractor, SimpleKeyExtractionError};
use actix_web::dev::ServiceRequest;
use crate::utils::jwt::verify_token;

#[derive(Clone, Debug)]
pub struct UserOrIpKeyExtractor;

impl KeyExtractor for UserOrIpKeyExtractor {
    type Key = String;
    type KeyExtractionError = SimpleKeyExtractionError<&'static str>;

    fn extract(&self, req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError> {
        if let Some(cfg) = req.app_data::<actix_web::web::Data<crate::config::Config>>() {
            if let Some(token) = req
                .headers()
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
            {
                if let Ok(claims) = verify_token(token, &cfg.jwt_secret) {
                    return Ok(format!("user:{}", claims.sub));
                }
            }
        }

        let ip = req
            .peer_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        Ok(format!("ip:{}", ip))
    }
}
use actix_governor::governor::middleware::StateInformationMiddleware;
use actix_governor::{GovernorConfig, GovernorConfigBuilder};
// Import your custom key extractor from where it is defined
use crate::utils::rate_limit::UserOrIpKeyExtractor; 

/// Strict: 3 req/min (1 req every 20s), burst of 2
/// Used for Auth, OTP, and Password reset
pub fn strict_governor() -> GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware> {
    GovernorConfigBuilder::default()
        .per_second(10) 
        .burst_size(5)
        .key_extractor(UserOrIpKeyExtractor) 
        .use_headers()
        .finish()
        .expect("Failed to build strict governor config")
}

/// Mutating: ~10 req/min, burst of 5
/// Used for account updates and profile changes
pub fn mutating_governor() -> GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware> {
    GovernorConfigBuilder::default()
        .per_second(6)
        .burst_size(5)
        .key_extractor(UserOrIpKeyExtractor) // Use your custom extractor here
        .use_headers()
        .finish()
        .expect("Failed to build mutating governor config")
}
use actix_governor::{Governor, GovernorConfig, governor::middleware::StateInformationMiddleware};
use actix_web::web;
use crate::modules::legacy_plan::handlers;
use crate::utils::rate_limit::UserOrIpKeyExtractor; 

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(
    cfg: &mut web::ServiceConfig, 
    mutating_gov: web::Data<GovConfig>
) {
    cfg.service(
        web::scope("/legacy-plan")
            // --- Mutating: POST, PUT, DELETE ---
            .route("", web::post().to(handlers::create_posthumous_plan).wrap(Governor::new(&mutating_gov)))
            .route("/{id}", web::put().to(handlers::update_legacy_plan).wrap(Governor::new(&mutating_gov)))
            .route("/{id}", web::delete().to(handlers::delete_legacy_plan).wrap(Governor::new(&mutating_gov)))
            
            .route("", web::get().to(handlers::get_legacy_plan)),
    );
}
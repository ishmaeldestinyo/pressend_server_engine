use actix_governor::{Governor, GovernorConfig, governor::middleware::StateInformationMiddleware};
use actix_web::web;
use crate::modules::beneficiary::handlers;
use crate::utils::rate_limit::UserOrIpKeyExtractor;

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(
    cfg: &mut web::ServiceConfig,
    mutating_gov: web::Data<GovConfig>
) {
    cfg.service(
        web::scope("/beneficiary")
            // ── Mutating: POST, PATCH, DELETE 
            .route("", web::post().to(handlers::add_beneficiary).wrap(Governor::new(&mutating_gov)))
            .route("/{id}", web::patch().to(handlers::update_beneficiary).wrap(Governor::new(&mutating_gov)))
            .route("/{id}", web::delete().to(handlers::delete_beneficiary).wrap(Governor::new(&mutating_gov)))
            
            // ── Read: GET 
            .route("", web::get().to(handlers::list_beneficiaries)),
    );
}
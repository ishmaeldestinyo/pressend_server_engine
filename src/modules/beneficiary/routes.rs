
use actix_web::web;
use crate::modules::beneficiary::handlers;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/beneficiary")
            .route("",          web::post().to(handlers::add_beneficiary))
            .route("",          web::get().to(handlers::list_beneficiaries))
            .route("/{id}",     web::patch().to(handlers::update_beneficiary))
            .route("/{id}",     web::delete().to(handlers::delete_beneficiary)),
    );
}
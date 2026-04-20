use actix_web::web;

use crate::modules::legacy_plan::handlers;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/legacy-plan")
            .route("", web::post().to(handlers::create_posthumous_plan))
            .route("", web::get().to(handlers::get_legacy_plan))
            .route("/{id}", web::put().to(handlers::update_legacy_plan))
            .route("/{id}", web::delete().to(handlers::delete_legacy_plan)),
    );
}

use actix_web::web;
use crate::modules::palm::handlers;



pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/palmprint")
        .route("/start",  web::post().to(handlers::set_palmprint_start))
        .route("/commit", web::post().to(handlers::set_palmprint_commit))
    );
}

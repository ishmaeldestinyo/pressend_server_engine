use actix_web::web;
use crate::modules::crypto::handlers;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/cryptocurrency").route("/", web::post().to(handlers::get_supported_cryptocurrencies)));
}
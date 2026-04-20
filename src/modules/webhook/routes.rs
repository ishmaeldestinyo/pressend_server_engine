use actix_web::web;
use crate::modules::webhook::handlers;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/9psb").route("/webhook", web::post().to(handlers::_9psb_webhook)));
}
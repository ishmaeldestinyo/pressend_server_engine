use crate::modules::transactions::handlers;
use actix_web::web;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/transaction")
            .route("/", web::get().to(handlers::list_mytransaction))
            .route("/banklist", web::get().to(handlers::list_bank))
            .route(
                "/resolve-bank",
                web::post().to(handlers::resolve_bank_detail),
            )
            .route(
                "/transfer/internal",
                web::post().to(handlers::internal_transfer),
            )
            .route("/fund", web::post().to(handlers::fund_wallet))
            .route("/success-rate", web::post().to(handlers::getbank_success_rate))
            .route(
                "/transfer/external",
                web::post().to(handlers::external_transfer),
            ),
    );
}

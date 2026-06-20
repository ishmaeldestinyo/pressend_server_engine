use actix_governor::{Governor, GovernorConfig, governor::middleware::StateInformationMiddleware};
use actix_web::web;
use crate::modules::transactions::handlers;
use crate::utils::rate_limit::UserOrIpKeyExtractor;

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(
    cfg: &mut web::ServiceConfig,
    mutating_gov: web::Data<GovConfig>
) {
    cfg.service(
        web::scope("/transaction")
            // ── Mutating: POST (Wrapped with Governor) ───────────────────
            .route("/resolve-bank", web::post().to(handlers::resolve_bank_detail).wrap(Governor::new(&mutating_gov)))
            .route("/grasp-intent", web::post().to(handlers::payment_grasp_intent).wrap(Governor::new(&mutating_gov)))
            .route("/transfer/internal", web::post().to(handlers::internal_transfer).wrap(Governor::new(&mutating_gov)))
            .route("/transfer/external", web::post().to(handlers::external_transfer).wrap(Governor::new(&mutating_gov)))
            .route("/fund", web::post().to(handlers::fund_wallet).wrap(Governor::new(&mutating_gov)))
            .route("/phantom-receive", web::post().to(handlers::phantom_receive).wrap(Governor::new(&mutating_gov)))
            .route("/success-rate", web::post().to(handlers::getbank_success_rate).wrap(Governor::new(&mutating_gov)))

            // ── Read: GET (No Governor) ──────────────────────────────────
            .route("/", web::get().to(handlers::list_mytransaction))
            .route("/banklist", web::get().to(handlers::list_bank)),
    );
}
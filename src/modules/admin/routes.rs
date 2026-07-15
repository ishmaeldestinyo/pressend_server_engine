use actix_governor::{Governor, GovernorConfig, governor::middleware::StateInformationMiddleware};
use actix_web::web;
use crate::modules::admin::handlers;
use crate::utils::rate_limit::UserOrIpKeyExtractor;
use crate::modules::admin::transactions::handlers::{top_transactions, search_transactions_between_accounts, volume_stats, channel_stats, get_transaction_details, list_transactions};
use crate::modules::admin::vas::handlers::{list_vas_transactions, vas_stats};
use crate::modules::admin::staff::handlers::{create_staff, reset_staff_password, list_staff};

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(
    cfg: &mut web::ServiceConfig,
    strict_gov: web::Data<GovConfig>,
    mutating_gov: web::Data<GovConfig>
) {
    cfg.service(
        web::scope("/admin")

        // --- AUTH ---
        // Keep this one strict — it's the endpoint an attacker actually wants
        // to brute-force, so it stays behind the tightest limiter.
        .route("/login", web::post().to(handlers::login).wrap(Governor::new(&strict_gov)))

        // --- MUTATING ROUTES ---
        // These still go through mutating_gov since they change state.
        .route("/users/{id}/review", web::patch().to(handlers::review_account).wrap(Governor::new(&mutating_gov)))
        .route("/staff", web::post().to(create_staff).wrap(Governor::new(&mutating_gov)))
        .route("/staff/{id}/reset-password", web::patch().to(reset_staff_password).wrap(Governor::new(&mutating_gov)))

        // --- READ ROUTES ---
        // No rate limiting here — a dashboard legitimately fires several of
        // these in parallel on every page load, and they're already behind
        // auth (the governor was never the thing protecting these from
        // unauthorized access, the JWT/session check is).
        .route("/users", web::get().to(handlers::get_accounts))
        .route("/users/{id}", web::get().to(handlers::get_account_details))
        .route("/users/{id}/wallet", web::get().to(handlers::get_account_wallet))

        .route("/roles", web::get().to(handlers::get_roles))
        .route("/staff", web::get().to(list_staff))

        .route("/transactions", web::get().to(list_transactions))
        .route("/transactions/top", web::get().to(top_transactions))
        .route("/transactions/search", web::get().to(search_transactions_between_accounts))
        .route("/transactions/stats/channel", web::get().to(channel_stats))
        .route("/transactions/stats/volume", web::get().to(volume_stats))
        .route("/transactions/{id}", web::get().to(get_transaction_details))

        .route("/vas-transactions", web::get().to(list_vas_transactions))
        .route("/vas-transactions/stats", web::get().to(vas_stats))
    );
}
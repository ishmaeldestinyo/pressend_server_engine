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
        .route("/login", web::post().to(handlers::login).wrap(Governor::new(&strict_gov)))

        // --- MUTATING ROUTES ---
        .route("/users/{id}/suspend", web::patch().to(handlers::suspend_account).wrap(Governor::new(&mutating_gov)))
        .route("/staff", web::post().to(create_staff).wrap(Governor::new(&mutating_gov)))
        .route("/staff/{id}/reset-password", web::patch().to(reset_staff_password).wrap(Governor::new(&mutating_gov)))

        // --- READ ROUTES ---
        .route("/users", web::get().to(handlers::get_accounts).wrap(Governor::new(&strict_gov)))
        .route("/users/{id}", web::get().to(handlers::get_account_details).wrap(Governor::new(&strict_gov)))
        .route("/users/{id}/wallet", web::get().to(handlers::get_account_wallet).wrap(Governor::new(&strict_gov)))

        .route("/roles", web::get().to(handlers::get_roles).wrap(Governor::new(&strict_gov)))
        .route("/staff", web::get().to(list_staff).wrap(Governor::new(&strict_gov)))

        .route("/transactions", web::get().to(list_transactions).wrap(Governor::new(&strict_gov)))
        .route("/transactions/top", web::get().to(top_transactions).wrap(Governor::new(&strict_gov)))
        .route("/transactions/search", web::get().to(search_transactions_between_accounts).wrap(Governor::new(&strict_gov)))
        .route("/transactions/stats/channel", web::get().to(channel_stats).wrap(Governor::new(&strict_gov)))
        .route("/transactions/stats/volume", web::get().to(volume_stats).wrap(Governor::new(&strict_gov)))
        .route("/transactions/{id}", web::get().to(get_transaction_details).wrap(Governor::new(&strict_gov)))

        .route("/vas-transactions", web::get().to(list_vas_transactions).wrap(Governor::new(&strict_gov)))
        .route("/vas-transactions/stats", web::get().to(vas_stats).wrap(Governor::new(&strict_gov)))
    );
}
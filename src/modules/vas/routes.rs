use actix_web::web;
use crate::modules::vas::handlers;



pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/vas")
            .route("/topup/network",               web::get().to(handlers::get_phone_network))
            .route("/topup/airtime",               web::post().to(handlers::airtime_purchase))
            .route("/topup/status",                web::get().to(handlers::get_topup_status))
            .route("/topup/data-plans",            web::get().to(handlers::get_data_plans))
            .route("/cabletv/fields", web::get().to(handlers::get_cabletv_fields))
            .route("/topup/data",                  web::post().to(handlers::data_purchase))
            .route("/network/status",              web::get().to(handlers::network_success_rates))
            .route("/transactions",                web::get().to(handlers::get_my_vas_transactions))
            .route("/bills/categories",            web::get().to(handlers::get_bill_categories))
            .route("/bills/billers/{category_id}", web::get().to(handlers::get_category_billers))
            .route("/bills/fields/{biller_id}",    web::get().to(handlers::get_biller_fields))
            .route("/bills/validate",              web::post().to(handlers::validate_biller))
            .route("/bills/pay",                   web::post().to(handlers::bills_payment))
            .route("/bills/status",                web::get().to(handlers::get_bills_payment_status))
    );
}
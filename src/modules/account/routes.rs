use crate::modules::account::handlers;
use actix_web::web;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/account")
            .route("/", web::post().to(handlers::signup))
            .route(
                "/set-payment-pin",
                web::post().to(handlers::set_payment_pin),
            )
            .route("/refresh", web::post().to(handlers::refresh_token))
            .route("/search", web::get().to(handlers::search_account))
            .route("/me", web::get().to(handlers::get_user_info))
            .route("/otp/send", web::post().to(handlers::send_otp))
            .route("/otp/verify", web::post().to(handlers::verify_otp))
            .route("/password/change", web::put().to(handlers::change_password))
            .route("/delete", web::delete().to(handlers::delete_account))
            .route("/signin", web::post().to(handlers::signin))
            .route(
                "/signin/new-device/verify",
                web::post().to(handlers::signin_new_device_verify),
            )
            .route(
                "/resetpassword/verify",
                web::post().to(handlers::reset_password_verify),
            )
            .route(
                "/resetpassword/submit",
                web::post().to(handlers::reset_password_submit),
            )
            .route(
                "/change-email/verify",
                web::post().to(handlers::verify_email_change),
            )
            .route(
                "/change-email/submit",
                web::post().to(handlers::change_email_submit),
            )
            .route("/open-wallet", web::post().to(handlers::open_wallet))
            .route("/wallet-enquiry", web::get().to(handlers::wallet_enquiry))
            .route(
                "/kyc-upgrade/tier2",
                web::post().to(handlers::upgrade_tier2),
            )
            .route(
                "/kyc-upgrade/tier3",
                web::post().to(handlers::upgrade_tier3),
            )
            .route("/panic/setup", web::post().to(handlers::toggle_panic))
            .route(
                "/device-token",
                web::patch().to(handlers::update_device_token),
            ),
    );
}

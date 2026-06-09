use actix_governor::{Governor, GovernorConfig, governor::middleware::StateInformationMiddleware};
use actix_web::web;
use crate::modules::account::handlers;
// Fixed import based on your compiler's suggestion:
use crate::utils::rate_limit::UserOrIpKeyExtractor; 

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(
    cfg: &mut web::ServiceConfig, 
    strict_gov: web::Data<GovConfig>, 
    mutating_gov: web::Data<GovConfig>
) {
    cfg.service(
        web::scope("/account")
            // --- STRICT ROUTES ---
            .route("/dojah/webhook", web::post().to(handlers::dojah_webhook).wrap(Governor::new(&strict_gov)))
            .route("/", web::post().to(handlers::signup).wrap(Governor::new(&strict_gov)))
            .route("/signin", web::post().to(handlers::signin).wrap(Governor::new(&strict_gov)))
            .route("/signin/new-device/verify", web::post().to(handlers::signin_new_device_verify).wrap(Governor::new(&strict_gov)))
            .route("/otp/send", web::post().to(handlers::send_otp).wrap(Governor::new(&strict_gov)))
            .route("/otp/verify", web::post().to(handlers::verify_otp).wrap(Governor::new(&strict_gov)))
            .route("/resetpassword/verify", web::post().to(handlers::reset_password_verify).wrap(Governor::new(&strict_gov)))
            .route("/resetpassword/submit", web::post().to(handlers::reset_password_submit).wrap(Governor::new(&strict_gov)))
            .route("/change-email/verify", web::post().to(handlers::verify_email_change).wrap(Governor::new(&strict_gov)))
            .route("/change-email/submit", web::post().to(handlers::change_email_submit).wrap(Governor::new(&strict_gov)))

            // --- MUTATING ROUTES ---
            .route("/refresh", web::post().to(handlers::refresh_token).wrap(Governor::new(&mutating_gov)))
            .route("/set-payment-pin", web::post().to(handlers::set_payment_pin).wrap(Governor::new(&mutating_gov)))
            .route("/palm/enroll", web::post().to(handlers::enroll_palm).wrap(Governor::new(&mutating_gov)))
            .route("/palm/verify", web::post().to(handlers::verify_palmpayment).wrap(Governor::new(&mutating_gov)))
            .route("/palm/revoke", web::patch().to(handlers::revoke_panic).wrap(Governor::new(&mutating_gov)))
            .route("/palm/{palm_type}", web::delete().to(handlers::delete_palm).wrap(Governor::new(&mutating_gov)))
            .route("/password/change", web::put().to(handlers::change_password).wrap(Governor::new(&mutating_gov)))
            .route("/delete", web::delete().to(handlers::delete_account).wrap(Governor::new(&mutating_gov)))
            .route("/kyc-upgrade/tier2", web::post().to(handlers::upgrade_tier2).wrap(Governor::new(&mutating_gov)))
            .route("/kyc-upgrade/tier3", web::post().to(handlers::upgrade_tier3).wrap(Governor::new(&mutating_gov)))
            .route("/device-token", web::patch().to(handlers::update_device_token).wrap(Governor::new(&mutating_gov)))

            // --- READ ROUTES ---
            .route("/search", web::get().to(handlers::search_account))
            .route("/me", web::get().to(handlers::get_user_info))
            .route("/palm", web::get().to(handlers::get_palm))
            .route("/palm/status", web::get().to(handlers::get_panic_status))
            .route("/wallet-enquiry", web::get().to(handlers::wallet_enquiry)),
    );
}
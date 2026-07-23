use crate::modules::push_notifications::handlers;
use crate::utils::rate_limit::UserOrIpKeyExtractor;
use actix_governor::{governor::middleware::StateInformationMiddleware, Governor, GovernorConfig};
use actix_web::web;

type GovConfig = GovernorConfig<UserOrIpKeyExtractor, StateInformationMiddleware>;

pub fn config(cfg: &mut web::ServiceConfig, mutating_gov: web::Data<GovConfig>) {
    cfg.service(
        web::scope("/push_notifications")
            // POST: Send New Notification
            .route(
                "/create",
                web::post()
                    .to(handlers::new_push_notification)
                    .wrap(Governor::new(&mutating_gov)),
            )
            // POST: Retry Failed Pushes for a Notification ID
            .route(
                "/{id}/retry",
                web::post()
                    .to(handlers::retry_failed_push_notification)
                    .wrap(Governor::new(&mutating_gov)),
            )
            // GET: Fetch all notifications
            .route("", web::get().to(handlers::get_all_push_notifications))
            // GET: Fetch single notification by ID
            .route("/{id}", web::get().to(handlers::get_push_notification_by_id))
            // PUT: Update notification details
            .route(
                "/{id}",
                web::put()
                    .to(handlers::update_push_notification)
                    .wrap(Governor::new(&mutating_gov)),
            )
            // DELETE: Delete notification entry
            .route(
                "/{id}",
                web::delete()
                    .to(handlers::delete_push_notification)
                    .wrap(Governor::new(&mutating_gov)),
            ),
    );
}
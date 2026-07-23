use actix_web::{web, HttpResponse, Responder};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::modules::push_notifications::schemas::{
    ApiResponse, PaginatedResponse, PaginationQuery, PushNotificationModel,
    PushNotificationStatus, SendPushNotificationSchema, UpdatePushNotificationSchema,
};
use crate::utils::fms::send_push_notification;

/// 1. Send push notification directly to all active accounts' registered devices
pub async fn new_push_notification(
    pool: web::Data<PgPool>,
    body: web::Json<SendPushNotificationSchema>,
) -> impl Responder {
    // 1. Save record into DB as 'processing'
    let insert_result = sqlx::query_as!(
        PushNotificationModel,
        r#"
        INSERT INTO push_notifications (title, body, data, target_group, status)
        VALUES ($1, $2, $3, $4, 'processing'::push_notification_status)
        RETURNING 
            id, title, body, data, target_group, 
            status AS "status: PushNotificationStatus", 
            scheduled_at, sent_at, total_recipients, 
            successful_sends, failed_sends, error_log, 
            created_by, created_at, updated_at
        "#,
        body.title,
        body.body,
        body.data.clone().unwrap_or(json!({})),
        body.target_group
    )
    .fetch_one(pool.get_ref())
    .await;

    let notification = match insert_result {
        Ok(record) => record,
        Err(err) => {
            return HttpResponse::InternalServerError().json(ApiResponse::<()> {
                success: false,
                message: format!("Failed to create notification record: {}", err),
                data: None,
            });
        }
    };

    // 2. Query active device tokens from the device_tokens table (one row per
    //    device, not one column per account) so every device a user is
    //    signed in on gets the push, not just whichever device logged in last.
    let device_tokens: Vec<String> = match sqlx::query_scalar!(
        r#"
        SELECT dt.device_token AS "device_token!"
        FROM device_tokens dt
        INNER JOIN accounts a ON a.id = dt.account_id
        WHERE a.status = 'active'
          AND a.deleted_at IS NULL
        "#
    )
    .fetch_all(pool.get_ref())
    .await {
        Ok(tokens) => tokens,
        Err(err) => {
            let _ = sqlx::query!(
                "UPDATE push_notifications SET status = 'failed'::push_notification_status, error_log = $1 WHERE id = $2",
                format!("Failed to fetch target tokens: {}", err),
                notification.id
            )
            .execute(pool.get_ref())
            .await;

            return HttpResponse::InternalServerError().json(ApiResponse::<()> {
                success: false,
                message: "Failed to retrieve user target tokens".to_string(),
                data: None,
            });
        }
    };

    let total_recipients = device_tokens.len() as i32;

    if total_recipients == 0 {
        let _ = sqlx::query!(
            r#"
            UPDATE push_notifications 
            SET status = 'sent'::push_notification_status, total_recipients = 0, successful_sends = 0, failed_sends = 0, sent_at = NOW() 
            WHERE id = $1
            "#,
            notification.id
        )
        .execute(pool.get_ref())
        .await;

        return HttpResponse::Ok().json(ApiResponse {
            success: true,
            message: "No active device tokens found. Notification marked as sent.".to_string(),
            data: Some(notification),
        });
    }

    // 3. Spawn background job for sending notifications asynchronously
    let db_pool = pool.get_ref().clone();
    let notification_id = notification.id;
    let title = body.title.clone();
    let body_text = body.body.clone();
    let payload = body.data.clone();

    tokio::spawn(async move {
        let mut success_count = 0i32;

        for token in &device_tokens {
            send_push_notification(
                token,
                &title,
                &body_text,
                payload.clone(),
            )
            .await;

            success_count += 1;
        }

        let fail_count = total_recipients - success_count;

        // Update database with final metrics
        let _ = sqlx::query!(
            r#"
            UPDATE push_notifications
            SET 
                status = 'sent'::push_notification_status,
                sent_at = NOW(),
                total_recipients = $1,
                successful_sends = $2,
                failed_sends = $3
            WHERE id = $4
            "#,
            total_recipients,
            success_count,
            fail_count,
            notification_id
        )
        .execute(&db_pool)
        .await;
    });

    // 4. Return immediate 200 OK response to avoid client timeouts
    HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Dispatching notification in background to {} recipient(s)", total_recipients),
        data: Some(notification),
    })
}

/// 2. Get all notifications (Paginated with optional status filter)
pub async fn get_all_push_notifications(
    pool: web::Data<PgPool>,
    query: web::Query<PaginationQuery>,
) -> impl Responder {
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * limit;

    let items = sqlx::query_as!(
        PushNotificationModel,
        r#"
        SELECT 
            id, title, body, data, target_group, 
            status AS "status: PushNotificationStatus", 
            scheduled_at, sent_at, total_recipients, 
            successful_sends, failed_sends, error_log, 
            created_by, created_at, updated_at
        FROM push_notifications
        WHERE ($1::push_notification_status IS NULL OR status = $1)
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#,
        query.status.clone() as Option<PushNotificationStatus>,
        limit,
        offset
    )
    .fetch_all(pool.get_ref())
    .await;

    let total_count = sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) 
        FROM push_notifications
        WHERE ($1::push_notification_status IS NULL OR status = $1)
        "#,
        query.status.clone() as Option<PushNotificationStatus>
    )
    .fetch_one(pool.get_ref())
    .await
    .unwrap_or(Some(0))
    .unwrap_or(0);

    match items {
        Ok(list) => HttpResponse::Ok().json(ApiResponse {
            success: true,
            message: "Notifications fetched successfully".to_string(),
            data: Some(PaginatedResponse {
                items: list,
                total: total_count,
                page,
                limit,
            }),
        }),
        Err(err) => HttpResponse::InternalServerError().json(ApiResponse::<()> {
            success: false,
            message: format!("Failed to fetch notifications: {}", err),
            data: None,
        }),
    }
}

/// 3. Get single notification by ID
pub async fn get_push_notification_by_id(
    pool: web::Data<PgPool>,
    path: web::Path<Uuid>,
) -> impl Responder {
    let id = path.into_inner();

    let result = sqlx::query_as!(
        PushNotificationModel,
        r#"
        SELECT 
            id, title, body, data, target_group, 
            status AS "status: PushNotificationStatus", 
            scheduled_at, sent_at, total_recipients, 
            successful_sends, failed_sends, error_log, 
            created_by, created_at, updated_at
        FROM push_notifications
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool.get_ref())
    .await;

    match result {
        Ok(Some(notification)) => HttpResponse::Ok().json(ApiResponse {
            success: true,
            message: "Notification details retrieved".to_string(),
            data: Some(notification),
        }),
        Ok(None) => HttpResponse::NotFound().json(ApiResponse::<()> {
            success: false,
            message: "Notification not found".to_string(),
            data: None,
        }),
        Err(err) => HttpResponse::InternalServerError().json(ApiResponse::<()> {
            success: false,
            message: format!("Database error: {}", err),
            data: None,
        }),
    }
}

/// 4. Update notification details
pub async fn update_push_notification(
    pool: web::Data<PgPool>,
    path: web::Path<Uuid>,
    body: web::Json<UpdatePushNotificationSchema>,
) -> impl Responder {
    let id = path.into_inner();

    let result = sqlx::query_as!(
        PushNotificationModel,
        r#"
        UPDATE push_notifications
        SET 
            title = COALESCE($1, title),
            body = COALESCE($2, body),
            data = COALESCE($3, data),
            target_group = COALESCE($4, target_group),
            status = COALESCE($5, status)
        WHERE id = $6
        RETURNING 
            id, title, body, data, target_group, 
            status AS "status: PushNotificationStatus", 
            scheduled_at, sent_at, total_recipients, 
            successful_sends, failed_sends, error_log, 
            created_by, created_at, updated_at
        "#,
        body.title,
        body.body,
        body.data,
        body.target_group,
        body.status.clone() as Option<PushNotificationStatus>,
        id
    )
    .fetch_optional(pool.get_ref())
    .await;

    match result {
        Ok(Some(updated)) => HttpResponse::Ok().json(ApiResponse {
            success: true,
            message: "Notification updated successfully".to_string(),
            data: Some(updated),
        }),
        Ok(None) => HttpResponse::NotFound().json(ApiResponse::<()> {
            success: false,
            message: "Notification not found".to_string(),
            data: None,
        }),
        Err(err) => HttpResponse::InternalServerError().json(ApiResponse::<()> {
            success: false,
            message: format!("Update failed: {}", err),
            data: None,
        }),
    }
}

/// 5. Delete notification
pub async fn delete_push_notification(
    pool: web::Data<PgPool>,
    path: web::Path<Uuid>,
) -> impl Responder {
    let id = path.into_inner();

    let result = sqlx::query!(
        r#"
        DELETE FROM push_notifications
        WHERE id = $1
        "#,
        id
    )
    .execute(pool.get_ref())
    .await;

    match result {
        Ok(res) if res.rows_affected() > 0 => HttpResponse::Ok().json(ApiResponse::<()> {
            success: true,
            message: "Notification deleted successfully".to_string(),
            data: None,
        }),
        Ok(_) => HttpResponse::NotFound().json(ApiResponse::<()> {
            success: false,
            message: "Notification not found".to_string(),
            data: None,
        }),
        Err(err) => HttpResponse::InternalServerError().json(ApiResponse::<()> {
            success: false,
            message: format!("Deletion failed: {}", err),
            data: None,
        }),
    }
}

/// 6. Retry sending push notification
pub async fn retry_failed_push_notification(
    pool: web::Data<PgPool>,
    path: web::Path<Uuid>,
) -> impl Responder {
    let id = path.into_inner();

    let notification = match sqlx::query_as!(
        PushNotificationModel,
        r#"
        SELECT 
            id, title, body, data, target_group, 
            status AS "status: PushNotificationStatus", 
            scheduled_at, sent_at, total_recipients, 
            successful_sends, failed_sends, error_log, 
            created_by, created_at, updated_at
        FROM push_notifications
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool.get_ref())
    .await
    {
        Ok(Some(record)) => record,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse::<()> {
                success: false,
                message: "Notification record not found".to_string(),
                data: None,
            });
        }
        Err(err) => {
            return HttpResponse::InternalServerError().json(ApiResponse::<()> {
                success: false,
                message: format!("Database error: {}", err),
                data: None,
            });
        }
    };

    // Same fix as new_push_notification: read from device_tokens (one row
    // per device) instead of the old single accounts.device_token column.
    let device_tokens: Vec<String> = match sqlx::query_scalar!(
        r#"
        SELECT dt.device_token AS "device_token!"
        FROM device_tokens dt
        INNER JOIN accounts a ON a.id = dt.account_id
        WHERE a.status = 'active'
          AND a.deleted_at IS NULL
        "#
    )
    .fetch_all(pool.get_ref())
    .await {
        Ok(tokens) => tokens,
        Err(err) => {
            return HttpResponse::InternalServerError().json(ApiResponse::<()> {
                success: false,
                message: format!("Failed to retrieve device tokens: {}", err),
                data: None,
            });
        }
    };

    if device_tokens.is_empty() {
        return HttpResponse::BadRequest().json(ApiResponse::<()> {
            success: false,
            message: "No active device tokens found to retry".to_string(),
            data: None,
        });
    }

    let _ = sqlx::query!(
        "UPDATE push_notifications SET status = 'processing'::push_notification_status WHERE id = $1",
        id
    )
    .execute(pool.get_ref())
    .await;

    // Background spawn for retry
    let db_pool = pool.get_ref().clone();
    let notification_id = notification.id;
    let title = notification.title.clone();
    let body_text = notification.body.clone();
    let payload = notification.data.clone();
    let total_recipients = device_tokens.len() as i32;

    tokio::spawn(async move {
        let mut success_count = 0i32;

        for token in &device_tokens {
            send_push_notification(
                token,
                &title,
                &body_text,
                payload.clone(),
            )
            .await;

            success_count += 1;
        }

        let fail_count = total_recipients - success_count;

        let _ = sqlx::query!(
            r#"
            UPDATE push_notifications
            SET 
                status = 'sent'::push_notification_status,
                sent_at = NOW(),
                total_recipients = $1,
                successful_sends = $2,
                failed_sends = $3,
                error_log = NULL
            WHERE id = $4
            "#,
            total_recipients,
            success_count,
            fail_count,
            notification_id
        )
        .execute(&db_pool)
        .await;
    });

    HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Retry task started in background for {} device(s)", total_recipients),
        data: Some(notification),
    })
}
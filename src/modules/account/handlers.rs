use crate::config::Config;
use crate::config::KafkaConfig;
use crate::kafka::KafkaProducer;
use crate::modules::account::schemas;
use crate::modules::account::schemas::OpenWalletRequest;
use crate::modules::account::schemas::UpdateDeviceTokenRequest;
use crate::utils::jwt::{AuthUser, generate_access_token, generate_refresh_token, verify_token};
use crate::utils::password_manager::hash_password;
use crate::utils::password_manager::verify_password;
use crate::utils::psb::PsbClient;
use crate::utils::responder::ValidationErrorResponse;
use crate::utils::responder::{ApiResponse, AuthResponse, ResponseStatus};

use crate::worker_events::SetPaymentPinEvent;
use crate::worker_events::{
    AccountLoggedInNotificationEvent, ChangeEmailEvent, ChangePasswordEvent, DeleteAccountEvent,
    NewDeviceLoginEvent, OpenWalletEvent, SendOTPEvent, SignupEvent, SuspiciousLoginEvent,
    Tier2UpgradeEvent, Tier3UpgradeEvent, UpdateDeviceIdEvent, VerifyEmailEvent,
};
use crate::worker_handlers::generate_otp;
use actix_web::HttpRequest;
use actix_web::{HttpResponse, Responder, web};
use sqlx::PgPool;
use uuid::Uuid;
use validator::Validate;


pub async fn signup(
    body: web::Json<schemas::SignupRequest>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let otp_redis_key = format!("{}.verify.otp", body.email);

    let event = SignupEvent {
        email: body.email.clone(),
        password: body.password.clone(),
        device_id: body.device_id.clone(),
        account_type: body.account_type.to_string(),
        otp_redis_key,
    };

    kafka.publish(&kafka_cfg.kafka_topic_account_signup, &body.email, &event);

    HttpResponse::Created().json(ApiResponse {
        message: "Check your inbox for an OTP".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn send_otp(
    body: web::Json<schemas::SendOTPRequest>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    /*
    Send OTP request handler
    */
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let otp_redis_key = format!("{}.verify.otp", body.email);

    let event = SendOTPEvent {
        email: body.email.clone(),
        otp_redis_key,
    };

    kafka.publish(&kafka_cfg.kafka_topic_account_otp_send, &body.email, &event);

    HttpResponse::Ok().json(ApiResponse {
        message: "Check your inbox for an OTP".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn verify_otp(
    body: web::Json<schemas::VerifyOTPRequest>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<redis::aio::ConnectionManager>,
    cfg: web::Data<Config>,
    db: web::Data<sqlx::PgPool>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let otp_redis_key = format!("{}.verify.otp", body.email);

    // ── Get OTP hash from Redis ───────────────────────────────────────────────
    let mut redis_conn = redis.get_ref().clone();
    let stored_hash: Option<String> = match redis::cmd("GET")
        .arg(&otp_redis_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            println!("[verify_otp] Redis error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let stored_hash = match stored_hash {
        Some(h) => h,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify OTP ────────────────────────────────────────────────────────────
    match verify_password(&body.otp, &stored_hash) {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Delete OTP + fetch account id concurrently ────────────────────────────
    let email = body.email.clone();
    let (_, account) = tokio::join!(
        async {
            let _: Result<(), redis::RedisError> = redis::cmd("DEL")
                .arg(&otp_redis_key)
                .query_async(&mut redis_conn)
                .await;
        },
        sqlx::query!("SELECT id FROM accounts WHERE email = $1", email).fetch_one(db.get_ref())
    );

    let account_id = match account {
        Ok(row) => row.id.to_string(),
        Err(e) => {
            println!("[verify_otp] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Generate tokens concurrently ──────────────────────────────────────────
    let secret = cfg.jwt_secret.clone();
    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) },
    );

    let access_token = match access_token {
        Ok(t) => t,
        Err(e) => {
            println!("[verify_otp] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let refresh_token = match refresh_token {
        Ok(t) => t,
        Err(e) => {
            println!("[verify_otp] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fire and forget — worker sets email_verified = true ───────────────────
    let event = VerifyEmailEvent {
        email: body.email.clone(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_account_otp_verify,
        &body.email,
        &event,
    );

    HttpResponse::Ok().json(AuthResponse {
        status: ResponseStatus::SUCCESS,
        message: "Email verified successfully".into(),
        access_token: Some(access_token),
        refresh_token: Some(refresh_token),
    })
}

pub async fn signin(
    body: web::Json<schemas::SignInRequest>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    cfg: web::Data<Config>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let mut redis_conn = redis.get_ref().clone();
    let email = body.email.trim().to_lowercase();

    const TRIALS_PER_BLOCK: i64 = 3;
    const LOCKOUT_SEQUENCE: &[i64] = &[1, 5, 15, 45, 135, 405, 1440, 4320];

    fn format_duration(minutes: i64) -> String {
        if minutes < 60 {
            format!("{} minute{}", minutes, if minutes != 1 { "s" } else { "" })
        } else if minutes < 1440 {
            let hours = minutes / 60;
            format!("{} hour{}", hours, if hours != 1 { "s" } else { "" })
        } else {
            let days = minutes / 1440;
            format!("{} day{}", days, if days != 1 { "s" } else { "" })
        }
    }

    let lockout_key = format!("lockout:{}", email);
    let trials_key = format!("trials:{}", email);
    let block_key = format!("lockout_block:{}", email);
    let ip = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();

    // ── Check if currently locked out ─────────────────────────────────────────
    let lockout_until: Option<String> = redis::cmd("GET")
        .arg(&lockout_key)
        .query_async(&mut redis_conn)
        .await
        .unwrap_or(None);

    if lockout_until.is_some() {
        let remaining_seconds: i64 = redis::cmd("TTL")
            .arg(&lockout_key)
            .query_async(&mut redis_conn)
            .await
            .unwrap_or(0);
        let remaining_minutes = (remaining_seconds + 59) / 60;
        return HttpResponse::TooManyRequests().json(ApiResponse {
            message: format!(
                "Account temporarily locked. Try again in {}.",
                format_duration(remaining_minutes)
            )
            .into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Fetch account ──────────────────────────────────────────────────────────
    let account = sqlx::query!(
        "SELECT id, email, password_hash, status, device_id, is_2fa_enabled, firstname
         FROM accounts WHERE email = $1 AND deleted_at IS NULL",
        email
    )
    .fetch_optional(db.get_ref())
    .await;

    let row = match account {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid email or password".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[signin] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify password on blocking thread ─────────────────────────────────────
    let password_hash = row.password_hash.clone();
    let input_password = body.password.clone();

    let is_valid =
        tokio::task::spawn_blocking(move || verify_password(&input_password, &password_hash))
            .await
            .unwrap_or(Ok(false));

    if !matches!(is_valid, Ok(true)) {
        // ── Increment failed attempts ──────────────────────────────────────────
        let current_trials: i64 = redis::cmd("INCR")
            .arg(&trials_key)
            .query_async(&mut redis_conn)
            .await
            .unwrap_or(1);

        let _: Result<(), _> = redis::cmd("EXPIRE")
            .arg(&trials_key)
            .arg(86400i64)
            .query_async(&mut redis_conn)
            .await;

        let attempts_left = TRIALS_PER_BLOCK - (current_trials % TRIALS_PER_BLOCK);

        // ── Boundary hit — trigger lockout ─────────────────────────────────────
        if current_trials % TRIALS_PER_BLOCK == 0 {
            let block = ((current_trials / TRIALS_PER_BLOCK) - 1)
                .min(LOCKOUT_SEQUENCE.len() as i64 - 1) as usize;
            let lockout_minutes = LOCKOUT_SEQUENCE[block];

            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&lockout_key)
                .arg(lockout_minutes * 60)
                .arg("locked")
                .query_async(&mut redis_conn)
                .await;

            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&block_key)
                .arg(lockout_minutes * 60 + 60)
                .arg(block.to_string())
                .query_async(&mut redis_conn)
                .await;

            // ── Fire suspicious login event ────────────────────────────────────
            let event = SuspiciousLoginEvent {
                account_id: row.id.to_string(),
                email: row.email.clone(),
                firstname: row.firstname.clone().unwrap_or_default(),
                current_trials,
                previous_block: block as i64,
                ip: ip.clone(),
            };
            kafka.publish(
                &kafka_cfg.kafka_topic_account_suspicious_login,
                &row.email,
                &event,
            );

            return HttpResponse::TooManyRequests().json(ApiResponse {
                message: format!(
                    "Too many failed attempts. Account locked for {}.",
                    format_duration(lockout_minutes)
                )
                .into(),
                status: ResponseStatus::ERROR,
            });
        }

        return HttpResponse::Unauthorized().json(ApiResponse {
            message: format!(
                "Invalid email or password. {} attempt{} remaining before lockout.",
                attempts_left,
                if attempts_left != 1 { "s" } else { "" }
            )
            .into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Successful login — clear all lockout state ─────────────────────────────
    let _: Result<(), _> = redis::cmd("DEL")
        .arg(&[&lockout_key, &trials_key, &block_key])
        .query_async(&mut redis_conn)
        .await;

    // ── Check account status ───────────────────────────────────────────────────
    match row.status.as_str() {
        "suspended" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Your account has been suspended. Please contact support.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        "deleted" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message:
                    "This account no longer exists. If this was a mistake, please contact support."
                        .into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {}
    }

    // ── New or different device — send OTP before granting tokens ──────────────
    if row.device_id.as_deref() != Some(body.device_id.as_str()) {
        let otp = generate_otp();
        let otp_key = format!("{}.new_device.otp", email);

        let _: Result<(), _> = redis::cmd("SETEX")
            .arg(&otp_key)
            .arg(600i64)
            .arg(&otp)
            .query_async(&mut redis_conn)
            .await;

        let event = NewDeviceLoginEvent {
            account_id: row.id.to_string(),
            email: row.email.clone(),
            firstname: row.firstname.clone().unwrap_or_default(),
            otp: otp.clone(),
            otp_redis_key: otp_key.clone(),
            ip: ip.clone(),
        };

        kafka.publish(
            &kafka_cfg.kafka_topic_account_new_device,
            &row.email,
            &event,
        );

        return HttpResponse::Forbidden().json(ApiResponse {
            message:
                "New device detected. Please verify your identity via the OTP sent to your email."
                    .into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── 2FA check ──────────────────────────────────────────────────────────────
    if row.is_2fa_enabled {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": "2FA verification required.",
            "requires_2fa": true,
        }));
    }
    let ip = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();

    // kafka sents login detected
    let event = AccountLoggedInNotificationEvent {
        email: email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        ip,
    };

    kafka.publish(&kafka_cfg.kafka_account_login_successful, &email, &event);

    // ── Generate tokens concurrently ───────────────────────────────────────────
    let account_id = row.id.to_string();
    let secret = cfg.jwt_secret.clone();

    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) },
    );

    let access_token = match access_token {
        Ok(t) => t,
        Err(e) => {
            println!("[signin] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let refresh_token = match refresh_token {
        Ok(t) => t,
        Err(e) => {
            println!("[signin] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    HttpResponse::Ok().json(AuthResponse {
        status: ResponseStatus::SUCCESS,
        message: "Sign in successful. Welcome back!".into(),
        access_token: Some(access_token),
        refresh_token: Some(refresh_token),
    })
}





pub async fn signin_new_device_verify(
    body: web::Json<schemas::VerifyOTPRequest>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    cfg: web::Data<Config>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    req: HttpRequest,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let email = body.email.trim().to_lowercase();
    let otp_key = format!("{}.new_device.otp", email);
    let mut redis_conn = redis.get_ref().clone();

    // ── Get OTP from Redis ────────────────────────────────────────────────────
    let stored_otp: Option<String> = match redis::cmd("GET")
        .arg(&otp_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            println!("[signin_new_device_verify] Redis error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let stored_otp = match stored_otp {
        Some(o) => o,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify OTP ────────────────────────────────────────────────────────────
    if stored_otp != body.otp {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Invalid or expired OTP".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Fetch account + DEL otp concurrently ──────────────────────────────────
    let (account, _): (Result<_, sqlx::Error>, Result<_, redis::RedisError>) = tokio::join!(
        sqlx::query!(
            "SELECT id, device_id, firstname FROM accounts WHERE email = $1 AND deleted_at IS NULL",
            email
        )
        .fetch_optional(db.get_ref()),
        async {
            redis::cmd("DEL")
                .arg(&otp_key)
                .query_async::<_, ()>(&mut redis_conn)
                .await
        }
    );

    let row = match account {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Invalid credentials".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[signin_new_device_verify] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Generate tokens concurrently ──────────────────────────────────────────
    let account_id = row.id.to_string();
    let secret = cfg.jwt_secret.clone();

    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) },
    );

    let access_token = match access_token {
        Ok(t) => t,
        Err(e) => {
            println!("[signin_new_device_verify] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let refresh_token = match refresh_token {
        Ok(t) => t,
        Err(e) => {
            println!("[signin_new_device_verify] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fire and forget — worker updates device_id ────────────────────────────
    let event = UpdateDeviceIdEvent {
        account_id: account_id.clone(),
        email: email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        new_device_id: body.device_id.clone().unwrap_or_default(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_account_device_update, &email, &event);
    let ip = req
        .connection_info()
        .realip_remote_addr()
        .unwrap_or("unknown")
        .to_string();
    // kafka sents login detected
    let event = AccountLoggedInNotificationEvent {
        email: email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        ip,
    };

    kafka.publish(&kafka_cfg.kafka_account_login_successful, &email, &event);

    HttpResponse::Ok().json(AuthResponse {
        status: ResponseStatus::SUCCESS,
        message: "Device verified successfully. Welcome back!".into(),
        access_token: Some(access_token),
        refresh_token: Some(refresh_token),
    })
}


pub async fn delete_account(
    auth: AuthUser,
    body: web::Json<schemas::CloseAccountRequest>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch account email ───────────────────────────────────────────────────
    let account = sqlx::query!(
        "SELECT email, firstname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_one(db.get_ref())
    .await;

    let row = match account {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid account credentials".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fire and forget — worker soft deletes account + sends email ───────────
    let event = DeleteAccountEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        firstname: row.firstname.clone(),
        deletion_reason: body.deletion_reason.clone(),
    };

    let cache_key = format!("account:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();

    kafka.publish(&kafka_cfg.kafka_topic_account_delete, &row.email, &event);

    let _: Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Gone().json(ApiResponse {
    message: "We're sorry to see you go. Your account has been scheduled for closure and all associated data will be removed within 24 hours. A confirmation has been sent to your email.".into(),
    status: ResponseStatus::SUCCESS,
})
}

pub async fn refresh_token(
    body: web::Json<schemas::RefreshTokenRequest>,
    cfg: web::Data<Config>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    // ── Verify refresh token ──────────────────────────────────────────────────
    let claims = match verify_token(&body.refresh_token, &cfg.jwt_secret) {
        Ok(c) => c,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid or expired refresh token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Ensure it's actually a refresh token ──────────────────────────────────
    if claims.token_type != "refresh" {
        return HttpResponse::Unauthorized().json(ApiResponse {
            message: "Invalid token type".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Generate both tokens concurrently ─────────────────────────────────────
    let secret = cfg.jwt_secret.clone();
    let id = claims.sub.clone();

    let (access_token, refresh_token) =
        tokio::join!(async { generate_access_token(&id, &secret) }, async {
            generate_refresh_token(&id, &secret)
        },);

    let access_token = match access_token {
        Ok(t) => t,
        Err(e) => {
            println!("[refresh_token] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let refresh_token = match refresh_token {
        Ok(t) => t,
        Err(e) => {
            println!("[refresh_token] Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    HttpResponse::Ok().json(AuthResponse {
        status: ResponseStatus::SUCCESS,
        message: "Token refreshed".into(),
        access_token: Some(access_token),
        refresh_token: Some(refresh_token),
    })
}

pub async fn get_user_info(
    auth: AuthUser,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let cache_key = format!("account:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check Redis cache first ───────────────────────────────────────────────
    let cached: Option<String> = match redis::cmd("GET")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(_) => None,
    };

    if let Some(data) = cached {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "data": serde_json::from_str::<serde_json::Value>(&data).unwrap_or_default()
        }));
    }

    // ── Cache miss — parse UUID ───────────────────────────────────────────────
    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch account + contact history concurrently ──────────────────────────
    let (account, contact_history): (Result<_, sqlx::Error>, Result<_, sqlx::Error>) = tokio::join!(
        sqlx::query!(
            r#"SELECT id, email, firstname, lastname, othername, phone_number,
               email_verified, phone_no_verified, status, account_type,
               is_2fa_enabled, device_id, current_tier, pending_tier_upgrade,
               tier_upgraded_at, tier_upgrade_requested_at, panic_enabled,
               panic_message, panic_activated_at, panic_deactivated_at,
               account_number, account_name, bvn, nin,
               nin_userid, created_at, updated_at
               FROM accounts WHERE id = $1"#,
            account_uuid
        )
        .fetch_one(db.get_ref()),
        sqlx::query!(
            r#"SELECT id, field, old_value, new_value, registered_at, changed_at
               FROM account_contact_history
               WHERE account_id = $1
               ORDER BY registered_at DESC"#,
            account_uuid
        )
        .fetch_all(db.get_ref())
    );

    let row = match account {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let history = match contact_history {
        Ok(rows) => rows
            .iter()
            .map(|h| {
                serde_json::json!({
                    "id": h.id,
                    "field": h.field,
                    "old_value": h.old_value,
                    "new_value": h.new_value,
                    "registered_at": h.registered_at,
                    "changed_at": h.changed_at,
                })
            })
            .collect::<Vec<_>>(),
        Err(_) => vec![],
    };

    let data = serde_json::json!({
        "id": row.id,
        "email": row.email,
        "firstname": row.firstname,
        "lastname": row.lastname,
        "othername": row.othername,
        "phone_number": row.phone_number,
        "email_verified": row.email_verified,
        "phone_no_verified": row.phone_no_verified,
        "status": row.status,
        "account_type": row.account_type,
        "is_2fa_enabled": row.is_2fa_enabled,
        "device_id": row.device_id,
        "current_tier": row.current_tier,
        "pending_tier_upgrade": row.pending_tier_upgrade,
        "tier_upgraded_at": row.tier_upgraded_at,
        "tier_upgrade_requested_at": row.tier_upgrade_requested_at,
        "panic_enabled": row.panic_enabled,
        "panic_message": row.panic_message,
        "panic_activated_at": row.panic_activated_at,
        "panic_deactivated_at": row.panic_deactivated_at,
        "account_number": row.account_number,
        "account_name": row.account_name,
        "bvn": row.bvn,
        "nin": row.nin,
        "nin_userid": row.nin_userid,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "contact_history": history,
    });

    // ── Store in Redis — TTL 5 minutes ────────────────────────────────────────
    let _: Result<(), redis::RedisError> = redis::cmd("SETEX")
        .arg(&cache_key)
        .arg(300u64)
        .arg(data.to_string())
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": data
    }))
}




pub async fn change_password(
    auth: AuthUser,
    body: web::Json<schemas::ChangePasswordRequest>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch current password hash ───────────────────────────────────────────
    let account = sqlx::query!(
        "SELECT email, password_hash FROM accounts WHERE id = $1",
        account_uuid
    )
    .fetch_one(db.get_ref())
    .await;

    let row = match account {
        Ok(r) => r,
        Err(_) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify current password on blocking thread ────────────────────────────
    let password_hash = row.password_hash.clone();
    let current_password = body.current_password.clone();

    let is_valid =
        tokio::task::spawn_blocking(move || verify_password(&current_password, &password_hash))
            .await
            .unwrap_or(Ok(false));

    match is_valid {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Current password is incorrect".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Fire and forget — worker hashes, updates DB, contact history, sends email
    let event = ChangePasswordEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        new_password: body.new_password.clone(), // plain — worker hashes it
    };

    let cache_key = format!("account:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();

    kafka.publish(
        &kafka_cfg.kafka_topic_password_change_submit,
        &row.email,
        &event,
    );

    let _: Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(ApiResponse {
        message: "Password changed successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn reset_password_verify(
    body: web::Json<schemas::ResetPasswordVerification>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let email = body.email.trim().to_lowercase();
    let otp_key = format!("{}.verify.otp", email);
    let reset_key = format!("{}.resetpassword", email);
    let mut redis_conn = redis.get_ref().clone();

    // ── Get OTP hash from Redis ───────────────────────────────────────────────
    let stored_hash: Option<String> = match redis::cmd("GET")
        .arg(&otp_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            println!("[reset_password_verify] Redis error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let stored_hash = match stored_hash {
        Some(h) => h,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify OTP on blocking thread ─────────────────────────────────────────
    let otp = body.otp.clone();
    let is_valid = tokio::task::spawn_blocking(move || verify_password(&otp, &stored_hash))
        .await
        .unwrap_or(Ok(false));

    match is_valid {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── DEL otp + store reset session concurrently ────────────────────────────
    let mut redis_conn2 = redis.get_ref().clone();

    let (_, set_result): (Result<(), redis::RedisError>, Result<(), redis::RedisError>) = tokio::join!(
        async {
            redis::cmd("DEL")
                .arg(&otp_key)
                .query_async::<_, ()>(&mut redis_conn)
                .await
        },
        async {
            redis::cmd("SETEX")
                .arg(&reset_key)
                .arg(900u64)
                .arg(&email)
                .query_async::<_, ()>(&mut redis_conn2)
                .await
        }
    );

    if let Err(e) = set_result {
        println!("[reset_password_verify] Redis set error: {}", e);
        return HttpResponse::InternalServerError().json(ApiResponse {
            message: "Service temporarily unavailable".into(),
            status: ResponseStatus::ERROR,
        });
    }

    HttpResponse::Ok().json(ApiResponse {
        message: "OTP verified. You may now reset your password.".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn reset_password_submit(
    body: web::Json<schemas::ResetPasswordSubmit>,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let email = body.email.trim().to_lowercase();
    let reset_key = format!("{}.resetpassword", email);
    let mut redis_conn = redis.get_ref().clone();

    // ── Check reset session exists in Redis ───────────────────────────────────
    let reset_session: Option<String> = match redis::cmd("GET")
        .arg(&reset_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            println!("[reset_password_submit] Redis error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    match reset_session {
        Some(stored_email) if stored_email == email => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Reset session expired or invalid. Please request a new OTP.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Fetch account ─────────────────────────────────────────────────────────
    let account = sqlx::query!(
        "SELECT id, firstname FROM accounts WHERE email = $1 AND deleted_at IS NULL",
        email
    )
    .fetch_optional(db.get_ref())
    .await;

    let row = match account {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[reset_password_submit] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fire and forget — worker hashes + updates password, sends email ───────
    let event = ChangePasswordEvent {
        account_id: row.id.to_string(),
        email: email.clone(),
        new_password: body.new_password.clone(),
    };

    // ── DEL reset session + publish concurrently ──────────────────────────────
    let del_cmd = redis::cmd("DEL").arg(&reset_key).to_owned();
    let (_, _): (Result<(), redis::RedisError>, ()) =
        tokio::join!(del_cmd.query_async::<_, ()>(&mut redis_conn), async {
            kafka.publish(
                &kafka_cfg.kafka_topic_password_change_submit,
                &email,
                &event,
            );
        });

    HttpResponse::Ok().json(ApiResponse {
        message:
            "Your password has been reset successfully. You can now sign in with your new password."
                .into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn verify_email_change(
    auth: AuthUser,
    body: web::Json<schemas::ResetPasswordVerification>,
    redis: web::Data<redis::aio::ConnectionManager>,
    _kafka: web::Data<KafkaProducer>,
    _kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let email = body.email.trim().to_lowercase();
    let otp_key = format!("{}.verify.otp", email);
    let mut redis_conn = redis.get_ref().clone();

    // ── Get OTP hash from Redis ───────────────────────────────────────────────
    let stored_hash: Option<String> = match redis::cmd("GET")
        .arg(&otp_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            println!("[verify_email_change] Redis error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let stored_hash = match stored_hash {
        Some(h) => h,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify OTP on blocking thread ─────────────────────────────────────────
    let otp = body.otp.clone();
    let is_valid = tokio::task::spawn_blocking(move || verify_password(&otp, &stored_hash))
        .await
        .unwrap_or(Ok(false));

    match is_valid {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Determine if this is current email or new email verification ──────────
    // Check if pending_email_change already exists (means this is new email OTP)
    let account_id = auth.id.clone();
    let pending_key = format!("{}.pending_email_change", account_id);
    let new_email_key = format!("{}.new_email", account_id);

    let pending_exists: Option<String> = match redis::cmd("GET")
        .arg(&pending_key)
        .query_async(&mut redis_conn)
        .await
    {
        Ok(v) => v,
        Err(_) => None,
    };

    let mut redis_conn2 = redis.get_ref().clone();

    if pending_exists.is_some() {
        // ── New email OTP verified — store new email ──────────────────────────
        let del_cmd = redis::cmd("DEL").arg(&otp_key).to_owned();
        let set_cmd = redis::cmd("SETEX")
            .arg(&new_email_key)
            .arg(900u64)
            .arg(&email)
            .to_owned();

        let (_, set_result): (Result<(), redis::RedisError>, Result<(), redis::RedisError>) = tokio::join!(
            del_cmd.query_async::<_, ()>(&mut redis_conn),
            set_cmd.query_async::<_, ()>(&mut redis_conn2)
        );

        if let Err(e) = set_result {
            println!("[verify_email_change] Redis set error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }

        HttpResponse::Ok().json(ApiResponse {
            message: "New email verified. Please submit to complete the change.".into(),
            status: ResponseStatus::SUCCESS,
        })
    } else {
        // ── Current email OTP verified — store pending change session ─────────
        let del_cmd = redis::cmd("DEL").arg(&otp_key).to_owned();
        let set_cmd = redis::cmd("SETEX")
            .arg(&pending_key)
            .arg(900u64)
            .arg(&email)
            .to_owned();

        let (_, set_result): (Result<(), redis::RedisError>, Result<(), redis::RedisError>) = tokio::join!(
            del_cmd.query_async::<_, ()>(&mut redis_conn),
            set_cmd.query_async::<_, ()>(&mut redis_conn2)
        );

        if let Err(e) = set_result {
            println!("[verify_email_change] Redis set error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }

        HttpResponse::Ok().json(ApiResponse {
            message: "Current email verified. Please verify your new email address.".into(),
            status: ResponseStatus::SUCCESS,
        })
    }
}

pub async fn change_email_submit(
    auth: AuthUser,
    db: web::Data<sqlx::PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    let account_id = auth.id.clone();
    let pending_key = format!("{}.pending_email_change", account_id);
    let new_email_key = format!("{}.new_email", account_id);
    let mut redis_conn = redis.get_ref().clone();
    let mut redis_conn2 = redis.get_ref().clone();

    // ── Get old email + new email from Redis ──────────────────────────────────
    let get_cmd1 = redis::cmd("GET").arg(&pending_key).to_owned();
    let get_cmd2 = redis::cmd("GET").arg(&new_email_key).to_owned();

    let (old_email, new_email): (
        Result<Option<String>, redis::RedisError>,
        Result<Option<String>, redis::RedisError>,
    ) = tokio::join!(
        get_cmd1.query_async::<_, Option<String>>(&mut redis_conn),
        get_cmd2.query_async::<_, Option<String>>(&mut redis_conn2)
    );

    let old_email = match old_email {
        Ok(Some(e)) => e,
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Session expired. Please restart the email change process.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let new_email = match new_email {
        Ok(Some(e)) => e,
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "New email not verified. Please verify your new email first.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Guard: new email must differ from current email ───────────────────────
    if old_email == new_email {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "New email address must be different from your current email.".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Guard: new email must not belong to any account (including deleted) ───
    let email_taken = sqlx::query_scalar!(
        "SELECT EXISTS(SELECT 1 FROM accounts WHERE email = $1)",
        new_email
    )
    .fetch_one(db.get_ref())
    .await;

    match email_taken {
        Ok(Some(true)) => {
            return HttpResponse::Conflict().json(ApiResponse {
                message: "This email address is already associated with another account.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[change_email_submit] DB error checking email uniqueness: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {}
    }

    // ── Fetch account ─────────────────────────────────────────────────────────
    let account_uuid = match uuid::Uuid::parse_str(&account_id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let account = sqlx::query!(
        "SELECT id, firstname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    let row = match account {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[change_email_submit] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fire and forget — worker updates email + contact history + sends email
    let event = ChangeEmailEvent {
        account_id: account_id.clone(),
        old_email: old_email.clone(),
        new_email: new_email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_email_change_submit,
        &old_email,
        &event,
    );

    // ── DEL both redis keys + account cache ───────────────────────────────────
    let cache_key = format!("account:{}", account_id);
    let del_cmd = redis::cmd("DEL")
        .arg(&[&pending_key, &new_email_key, &cache_key])
        .to_owned();
    let _: Result<(), redis::RedisError> = del_cmd.query_async(&mut redis_conn).await;

    HttpResponse::Ok().json(ApiResponse {
        message: "Your email address has been updated successfully.".into(),
        status: ResponseStatus::SUCCESS,
    })
}



pub async fn open_wallet(
    auth: AuthUser,
    body: web::Json<OpenWalletRequest>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    let account_id = auth.id.clone();

    // ── Validate request ──────────────────────────────────────────────────────
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    if let Err(e) = body.0.validate_nin_or_bvn() {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: e.message.unwrap_or("Validation failed".into()).to_string(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Fire and forget ───────────────────────────────────────────────────────
    let event = OpenWalletEvent {
        account_id: account_id.clone(),
        firstname: body.firstname.clone(),
        lastname: body.lastname.clone(),
        othername: body.othername.clone().unwrap_or_default(),
        phone_no: body.phone_no.clone(),
        gender: body.gender.clone(),
        date_of_birth: body.date_of_birth.clone(),
        address: body.address.clone(),
        nin: body.nin.clone(),
        nin_userid: body.nin_userid.clone(),
        bvn: body.bvn.clone(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_open_wallet, &account_id, &event);

    HttpResponse::Accepted().json(ApiResponse {
        message: "Your security threat check is been validated".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn upgrade_tier2(
    auth: AuthUser,
    body: web::Json<schemas::UpgradeTier2Request>,
    db: web::Data<PgPool>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    let account_id = auth.id.clone();

    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let pep = body.pep.to_uppercase();
    if pep != "YES" && pep != "NO" {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: "PEP must be YES or NO".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&account_id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account ID".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch saved tier 1 fields from DB ─────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT bvn, nin, phone_number, current_tier, pending_tier_upgrade, status
         FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(_) => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if row.status != "active" {
        return HttpResponse::Forbidden().json(ApiResponse {
            message: "Account is not active".into(),
            status: ResponseStatus::ERROR,
        });
    }

    if row.current_tier < 1 {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "You must complete tier 1 upgrade before applying for tier 2".into(),
            status: ResponseStatus::ERROR,
        });
    }

    if row.current_tier >= 2 {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Account is already tier 2 or higher".into(),
            status: ResponseStatus::ERROR,
        });
    }

    if row.pending_tier_upgrade.is_some() {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "A tier upgrade is already in progress".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Merge: use request value if provided, fall back to saved DB value ─────
    let bvn = body.bvn.clone().or(row.bvn.clone()).unwrap_or_default();
    let nin = body.nin.clone().or(row.nin.clone()).unwrap_or_default();
    let phone_no = body
        .phone_no
        .clone()
        .or(row.phone_number.clone())
        .unwrap_or_default();


    let event = Tier2UpgradeEvent {
        account_id: account_id.clone(),
        bvn,
        nin,
        phone_no,
        id_type: body.id_type,
        id_number: body.id_number.clone(),
        id_issue_date: body.id_issue_date.clone(),
        id_expiry_date: body.id_expiry_date.clone(),
        house_number: body.house_number.clone(),
        street_name: body.street_name.clone(),
        state: body.state.clone(),
        city: body.city.clone(),
        local_government: body.local_government.clone(),
        nearest_landmark: body.nearest_landmark.clone(),
        place_of_birth: body.place_of_birth.clone(),
        pep,
        user_photo: body.user_photo.clone(),
        id_card_front: body.id_card_front.clone(),
        id_card_back: body.id_card_back.clone(),
        customer_signature: body.customer_signature.clone(),
        utility_bill: body.utility_bill.clone(),
        proof_of_address: body.proof_of_address.clone(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_tier2_upgrade, &account_id, &event);

    HttpResponse::Accepted().json(ApiResponse {
        message: "Tier 2 upgrade request submitted. You will be notified once processed.".into(),
        status: ResponseStatus::SUCCESS,
    })
}


pub async fn upgrade_tier3(
    auth: AuthUser,
    body: web::Json<schemas::UpgradeTier3Request>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    let account_id = auth.id.clone();

    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&account_id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account ID".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let event = Tier3UpgradeEvent {
        account_id: account_uuid.to_string(),
        bvn: body.bvn.clone(),
        nin: body.nin.clone(),
        proof_of_address: body.proof_of_address.clone(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_tier3_upgrade, &account_id, &event);

    HttpResponse::Accepted().json(ApiResponse {
        message: "Tier 3 upgrade request submitted. You will be notified once processed.".into(),
        status: ResponseStatus::SUCCESS,
    })
}


pub async fn search_account(
    auth: AuthUser,
    query: web::Query<schemas::SearchAccountQuery>,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    if let Err(errors) = query.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let q = query.q.trim().to_lowercase();
    let like_q = format!("%{}%", q);
    let cache_key = format!("search:{}", q);
    let mut redis_conn = redis.get_ref().clone();

    // 1. Try Cache
    let cached: Option<String> = redis::cmd("GET")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await
        .unwrap_or(None);

    let accounts: Vec<serde_json::Value> = if let Some(cached_str) = cached {
        let all: Vec<serde_json::Value> = serde_json::from_str(&cached_str).unwrap_or_default();
        all.into_iter()
            .filter(|a| a["id"].as_str() != Some(&auth.id))
            .collect()
    } else {
        // 2. Optimized SQL
        let results = sqlx::query!(
            r#"
            SELECT
                id, firstname, lastname, othername, phone_number,
                email, account_number, account_name, account_type, current_tier
            FROM accounts
            WHERE
                deleted_at IS NULL
                AND (
                    -- Search by identifiers
                    TRIM(account_number::TEXT) = $1
                    OR TRIM(phone_number) = $1
                    OR LOWER(TRIM(email)) = $1

                    -- Search by the name part after '/'
                    OR LOWER(SPLIT_PART(account_name, '/', 2)) LIKE $2
                    
                    -- Search the whole name (fallback for names without / or with spaces)
                    OR LOWER(account_name) LIKE $2
                    
                    -- Individual name fields
                    OR LOWER(firstname) LIKE $2
                    OR LOWER(lastname) LIKE $2
                )
            LIMIT 11
            "#,
            q,      // $1
            like_q, // $2
        )
        .fetch_all(db.get_ref())
        .await;

        match results {
            Ok(rows) => {
                let all: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|r| {
                        let fullname = [
                            r.firstname.as_deref().unwrap_or(""),
                            r.othername.as_deref().unwrap_or(""),
                            r.lastname.as_deref().unwrap_or(""),
                        ]
                        .iter()
                        .filter(|s| !s.is_empty())
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" ");

                        serde_json::json!({
                            "id":             r.id,
                            "fullname":       fullname.trim(),
                            "account_number": r.account_number,
                            "account_name":   r.account_name,
                            "account_type":   r.account_type,
                            "current_tier":   r.current_tier,
                             "firstname":      r.firstname,
                            "lastname":       r.lastname,
                            "othername":      r.othername,
                            "email":          r.email,
                            "phone_number":   r.phone_number,
                        })
                    })
                    .collect();

                // 3. Cache results if found
                if !all.is_empty() {
                    let _: Result<(), _> = redis::cmd("SETEX")
                        .arg(&cache_key)
                        .arg(60u64)
                        .arg(serde_json::to_string(&all).unwrap_or_default())
                        .query_async::<_, ()>(&mut redis_conn)
                        .await;
                }

                // 4. IMPORTANT: Check if the ONLY result is the current user
                all.into_iter()
                    .filter(|a| a["id"].as_str() != Some(&auth.id))
                    .collect()
            }
            Err(e) => {
                log::error!("DB Error: {}", e);
                return HttpResponse::InternalServerError().finish();
            }
        }
    };

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": accounts,
    }))
}


pub async fn toggle_panic(
    auth: AuthUser,
    body: web::Json<schemas::TogglePanicRequest>,
    db: web::Data<PgPool>,
) -> impl Responder {
    let enabled = body.enabled;

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid account ID".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let result = if enabled {
        sqlx::query!(
            r#"UPDATE accounts SET
                panic_enabled = TRUE,
                panic_message = $1,
                panic_activated_at = NOW(),
                panic_deactivated_at = NULL,
                updated_at = NOW()
            WHERE id = $2"#,
            body.message.clone().unwrap_or("Service temporarily unavailable".into()),
            account_uuid
        )
        .execute(db.get_ref())
        .await
    } else {
        sqlx::query!(
            r#"UPDATE accounts SET
                panic_enabled = FALSE,
                panic_message = NULL,
                panic_deactivated_at = NOW(),
                updated_at = NOW()
            WHERE id = $1"#,
            account_uuid
        )
        .execute(db.get_ref())
        .await
    };

    match result {
        Ok(_) => HttpResponse::Ok().json(ApiResponse {
            message: if enabled {
                "Panic mode activated".into()
            } else {
                "Panic mode deactivated".into()
            },
            status: ResponseStatus::SUCCESS,
        }),
        Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
            message: "Failed to update panic mode".into(),
            status: ResponseStatus::ERROR,
        }),
    }
}




pub async fn set_payment_pin(
    auth:      AuthUser,
    body:      web::Json<schemas::SetPaymentPinRequest>,
    db:        web::Data<PgPool>,
    redis:     web::Data<redis::aio::ConnectionManager>,
    kafka:     web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status:  "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch account ─────────────────────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT email, password_hash, firstname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status:  ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[set_payment_pin] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    // ── Verify current password ───────────────────────────────────────────────
    let password_hash  = row.password_hash.clone();
    let input_password = body.current_password.clone();

    let is_valid =
        tokio::task::spawn_blocking(move || verify_password(&input_password, &password_hash))
            .await
            .unwrap_or(Ok(false));

    match is_valid {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Current password is incorrect".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    }

    // ── Hash new PIN ──────────────────────────────────────────────────────────
    let plain_pin = body.new_pin.clone();

    let pin_hash = match tokio::task::spawn_blocking(move || hash_password(&plain_pin)).await {
        Ok(Ok(h)) => h,
        _ => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    // ── Persist hashed PIN ────────────────────────────────────────────────────
    if let Err(e) = sqlx::query!(
        "UPDATE accounts SET pin_hash = $1, updated_at = NOW() WHERE id = $2",
        pin_hash,
        account_uuid
    )
    .execute(db.get_ref())
    .await
    {
        println!("[set_payment_pin] DB update error: {}", e);
        return HttpResponse::InternalServerError().json(ApiResponse {
            message: "Service temporarily unavailable".into(),
            status:  ResponseStatus::ERROR,
        });
    }

    // ── Fire Kafka ────────────────────────────────────────────────────────────
    let event = SetPaymentPinEvent {
        account_id: auth.id.clone(),
        email:      row.email.clone(),
        firstname:  row.firstname.clone().unwrap_or_default(),
    };

    kafka.publish(
        &kafka_cfg.kafka_topic_payment_pin_set,
        &row.email,
        &event,
    );

    // ── Bust both caches ──────────────────────────────────────────────────────
    let mut redis_conn       = redis.get_ref().clone();
    let account_cache_key    = format!("account:{}", auth.id);
    let pin_hash_cache_key   = format!("pin_hash:{}", account_uuid);

    let _: Result<(), _> = redis::cmd("DEL")
        .arg(&account_cache_key)
        .arg(&pin_hash_cache_key)  // ← bust both in one call
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(ApiResponse {
        message: "Payment PIN set successfully".into(),
        status:  ResponseStatus::SUCCESS,
    })
}

pub async fn update_device_token(
    auth: AuthUser,
    body: web::Json<UpdateDeviceTokenRequest>,
    db:   web::Data<PgPool>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status:  "error",
            message: "Invalid input",
            errors,
        });
    }

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    match sqlx::query!(
        "UPDATE accounts SET device_token = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL",
        body.device_token,
        account_uuid,
    )
    .execute(db.get_ref())
    .await
    {
        Ok(_) => HttpResponse::Ok().json(ApiResponse {
            message: "Device token updated".into(),
            status:  ResponseStatus::SUCCESS,
        }),
        Err(e) => {
            log::error!("[update_device_token] DB error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status:  ResponseStatus::ERROR,
            })
        }
    }
}


pub async fn wallet_enquiry(
    auth:  AuthUser,
    db:    web::Data<PgPool>,
    cfg:   web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch account number + status ─────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT account_number, status FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[wallet_enquiry] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    // ── Status guard ──────────────────────────────────────────────────────────
    match row.status.as_str() {
        "active" => {}
        "suspended" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Your account has been suspended. Please contact support.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
        "deleted" => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "This account no longer exists. If this was a mistake, please contact support.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
        _ => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Account is not active.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    }

    // ── Must have wallet ──────────────────────────────────────────────────────
    let account_number = match row.account_number {
        Some(n) => n,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    let mut redis_conn = redis.get_ref().clone();

    let response = match PsbClient::new(&cfg)
        .post(
            &mut redis_conn,
            "/waas/api/v1/wallet_enquiry",
            &serde_json::json!({ "accountNo": account_number }),
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("[wallet_enquiry] PSB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Unable to fetch wallet details at this time.".into(),
                status:  ResponseStatus::ERROR,
            });
        }
    };

    let status = response["status"].as_str().unwrap_or("").to_uppercase();
    if status != "SUCCESS" {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Unable to retrieve wallet information.".into(),
            status:  ResponseStatus::ERROR,
        });
    }

    // ── Inject limits based on tier from PSB response ─────────────────────────
    let tier = response["data"]["tier"]
        .as_str()
        .unwrap_or("0")
        .parse::<i32>()
        .unwrap_or(0);

    let limits = match tier {
        1 => serde_json::json!({
            "tier":              1,
            "single_deposit":    50_000,
            "daily_transaction": 50_000,
            "maximum_balance":   300_000,
        }),
        2 => serde_json::json!({
            "tier":              2,
            "single_deposit":    200_000,
            "daily_transaction": 200_000,
            "maximum_balance":   500_000,
        }),
        3 => serde_json::json!({
            "tier":              3,
            "single_deposit":    null,
            "daily_transaction": null,
            "maximum_balance":   null,
        }),
        _ => serde_json::json!({
            "tier":              0,
            "single_deposit":    null,
            "daily_transaction": null,
            "maximum_balance":   null,
        }),
    };

    // ── Merge limits into data ────────────────────────────────────────────────
    let mut data = response["data"].clone();
    data["limits"] = limits;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data":   data,
    }))
}




use crate::config::Config;
use crate::config::KafkaConfig;
use crate::kafka::KafkaProducer;
use crate::modules::account::schemas;
use crate::modules::account::schemas::Country;
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
    NewDeviceLoginEvent, SendOTPEvent, SignupEvent, SuspiciousLoginEvent, Tier2UpgradeEvent,
    Tier3UpgradeEvent, UpdateDeviceIdEvent, VerifyEmailEvent,
};
use crate::worker_handlers::generate_otp;
use actix_web::HttpRequest;
use actix_web::{HttpResponse, Responder, web};
use redis::aio::ConnectionManager;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::types::BigDecimal;
use std::str::FromStr;
use uuid::Uuid;
use validator::Validate;

pub async fn dojah_webhook(body: web::Json<Value>, db: web::Data<PgPool>) -> impl Responder {
    let body = body.into_inner();

    let verification_status = body["verification_status"].as_str().unwrap_or("");
    // if verification_status != "Completed" {
    //     return HttpResponse::Ok().json(
    //         serde_json::json!({
    //         "status": "success",
    //         "message": "Acknowledged"
    //     })
    //     );
    // }

    let reference_id = match body["reference_id"].as_str() {
        Some(r) => r.to_string(),
        None => {
            return HttpResponse::BadRequest().json(serde_json::json!({
                "status": "error",
                "message": "Missing reference_id"
            }));
        }
    };

    let widget_id = body["widget_id"].as_str().map(str::to_string);
    let id_type = body["id_type"].as_str().unwrap_or("").to_string();
    let verification_type = body["verification_type"].as_str().unwrap_or("").to_string();
    let verification_mode = body["verification_mode"].as_str().unwrap_or("").to_string();
    let verification_url = body["verification_url"].as_str().map(str::to_string);
    let value = body["value"].as_str().map(str::to_string);
    let status = body["status"].as_bool().unwrap_or(false);
    let aml_status = body["aml"]["status"].as_bool().unwrap_or(false);
    let message = body["message"].as_str().map(str::to_string);
    let selfie_url = body["selfie_url"].as_str().map(str::to_string);
    let device_info = body["metadata"]["device_info"].as_str().map(str::to_string);
    let ip_info = body["metadata"]["ipinfo"].clone();

    let liveness_score = body["data"]["selfie"]["data"]["liveness_score"]
        .as_f64()
        .and_then(|v| BigDecimal::from_str(&v.to_string()).ok());

    let match_score = body["data"]["selfie"]["data"]["match_score"]
        .as_f64()
        .and_then(|v| BigDecimal::from_str(&v.to_string()).ok());

    let verified_at = if status {
        Some(chrono::Utc::now())
    } else {
        None
    };

    let kyc_id: Uuid = match sqlx::query_scalar!(
        r#"
        INSERT INTO kyc_verifications (
            reference_id, widget_id,
            id_type, verification_type, verification_mode,
            verification_status, verification_url, value,
            status, aml_status, message,
            selfie_url, liveness_score, match_score,
            device_info, ip_info, raw_payload, verified_at
        )
        VALUES (
            $1, $2,
            $3, $4, $5,
            $6, $7, $8,
            $9, $10, $11,
            $12, $13, $14,
            $15, $16, $17, $18
        )
        RETURNING id
        "#,
        reference_id,
        widget_id,
        id_type,
        verification_type,
        verification_mode,
        verification_status,
        verification_url,
        value,
        status,
        aml_status,
        message,
        selfie_url,
        liveness_score,
        match_score,
        device_info,
        ip_info,
        body,
        verified_at
    )
    .fetch_one(db.get_ref())
    .await
    {
        Ok(id) => id,
        Err(e) => {
            eprintln!("[dojah_webhook] DB insert error: {}", e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": "Failed to store verification"
            }));
        }
    };

    match id_type.to_uppercase().as_str() {
        "NIN" => {
            let entity = &body["data"]["government_data"]["data"]["nin"]["entity"];

            let nin = entity["nin"].as_str().unwrap_or("").to_string();
            let first_name = entity["first_name"].as_str().map(str::to_string);
            let middle_name = entity["middle_name"].as_str().map(str::to_string);
            let last_name = entity["last_name"].as_str().map(str::to_string);
            let gender = entity["gender"].as_str().map(str::to_string);
            let phone_number = entity["phone_number"].as_str().map(str::to_string);
            let image_url = entity["image_url"].as_str().map(str::to_string);
            let app_id = entity["app_id"].as_str().map(str::to_string);
            let customer_ref = entity["customer"].as_str().map(str::to_string);

            let date_of_birth = entity["date_of_birth"]
                .as_str()
                .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

            if let Err(e) = sqlx::query!(
                r#"
                INSERT INTO kyc_nin_data (
                    kyc_verification_id, nin,
                    first_name, middle_name, last_name,
                    gender, date_of_birth, phone_number,
                    image_url, app_id, customer_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                kyc_id,
                nin,
                first_name,
                middle_name,
                last_name,
                gender,
                date_of_birth,
                phone_number,
                image_url,
                app_id,
                customer_ref
            )
            .execute(db.get_ref())
            .await
            {
                eprintln!("[dojah_webhook] NIN data insert error: {}", e);
            }
        }

        "BVN" => {
            let entity = &body["data"]["government_data"]["data"]["bvn"]["entity"];

            let bvn = entity["bvn"].as_str().unwrap_or("").to_string();
            let first_name = entity["first_name"].as_str().map(str::to_string);
            let middle_name = entity["middle_name"].as_str().map(str::to_string);
            let last_name = entity["last_name"].as_str().map(str::to_string);
            let gender = entity["gender"].as_str().map(str::to_string);
            let phone_number = entity["phone_number1"]
                .as_str()
                .or_else(|| entity["phone_number2"].as_str())
                .map(str::to_string);
            let image_url = entity["image_url"].as_str().map(str::to_string);
            let app_id = entity["app_id"].as_str().map(str::to_string);
            let customer_ref = entity["customer"].as_str().map(str::to_string);

            let date_of_birth = entity["date_of_birth"]
                .as_str()
                .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

            if let Err(e) = sqlx::query!(
                r#"
                INSERT INTO kyc_bvn_data (
                    kyc_verification_id, bvn,
                    first_name, middle_name, last_name,
                    gender, date_of_birth, phone_number,
                    image_url, app_id, customer_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
                kyc_id,
                bvn,
                first_name,
                middle_name,
                last_name,
                gender,
                date_of_birth,
                phone_number,
                image_url,
                app_id,
                customer_ref
            )
            .execute(db.get_ref())
            .await
            {
                eprintln!("[dojah_webhook] BVN data insert error: {}", e);
            }
        }

        other => {
            eprintln!("[dojah_webhook] Unhandled id_type: {}", other);
        }
    }

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Verification recorded"
    }))
}

pub async fn signup(
    body: web::Json<schemas::SignupRequest>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
    redis: web::Data<ConnectionManager>,
    db: web::Data<PgPool>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    let mut _redis_conn = redis.get_ref().clone();

    // ── Resolve referral code → referrer_id ──────────────────────────────────
    let referrer_id: Option<String> = if let Some(code) = &body.referral_code {
        match sqlx::query_scalar!(
            "SELECT id::TEXT FROM accounts WHERE referral_code = $1 AND deleted_at IS NULL LIMIT 1",
            code
        )
        .fetch_optional(db.get_ref())
        .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                return HttpResponse::BadRequest().json(serde_json::json!({
                    "status": "error",
                    "message": "Invalid referral code"
                }));
            }
            Err(e) => {
                eprintln!("[signup] referral code lookup error: {:?}", e);
                return HttpResponse::InternalServerError().json(serde_json::json!({
                    "status": "error",
                    "message": "Something went wrong. Please try again."
                }));
            }
        }
    } else {
        None
    };

    // ── Duplicate check — email only, since identity (NIN/BVN) isn't
    //    resolved yet. KYC dedup happens later when verification completes. ──
    let existing = sqlx::query!(
        "SELECT email, email_verified FROM accounts WHERE email = $1 LIMIT 1",
        body.email
    )
    .fetch_optional(db.get_ref())
    .await;

    match existing {
        Ok(Some(row)) => {
            if !row.email_verified {
                let otp_redis_key = format!("{}.verify.otp", body.email);
                let event = SendOTPEvent {
                    email: body.email.clone(),
                    otp_redis_key,
                };
                kafka.publish(&kafka_cfg.kafka_topic_account_otp_send, &body.email, &event);

                return HttpResponse::Ok().json(ApiResponse {
                    message: "Check your inbox for an OTP".into(),
                    status: ResponseStatus::SUCCESS,
                });
            }

            return HttpResponse::Conflict().json(serde_json::json!({
                "status": "error",
                "message": "An account with this email already exists"
            }));
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!("[signup] DB error: {:?}", e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": "Something went wrong. Please try again."
            }));
        }
    }

    // ── Build and publish signup event ────────────────────────────────────────
    let otp_redis_key = format!("{}.verify.otp", body.email);

    let event = SignupEvent {
        email: body.email.clone(),
        password: body.password.clone(),
        device_id: body.device_id.clone(),
        account_type: body.account_type.to_string(),
        kyc_reference: body.reference.clone(),
        otp_redis_key,
        nin: None,
        bvn: None,
        firstname: None,
        lastname: None,
        middlename: None,
        date_of_birth: None,
        gender: None,
        mobile_number: None,
        address: None,
        city: None,
        state: None,
        lga: None,
        user_photo: None,
        referrer_id,
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

    match verify_password(&body.otp, &stored_hash) {
        Ok(true) => {}
        _ => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Invalid or expired OTP".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    let email = body.email.clone();
    let (_, account) = tokio::join!(
        async {
            let _: Result<(), redis::RedisError> = redis::cmd("DEL")
                .arg(&otp_redis_key)
                .query_async(&mut redis_conn)
                .await;
        },
        sqlx::query!(
            r#"SELECT id, email, firstname, lastname, address, account_number, kyc_reference
               FROM accounts WHERE email = $1"#,
            email
        )
        .fetch_one(db.get_ref())
    );

    let account = match account {
        Ok(row) => row,
        Err(e) => {
            println!("[verify_otp] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let account_id = account.id.to_string();

    // ── Open 9PSB wallet synchronously if not already opened ─────────────────
    if account.account_number.is_none() {
        if let Some(kyc_reference) = account.kyc_reference.clone() {
            match sqlx::query!(
                r#"
                SELECT
                    kv.id_type                       AS "id_type!",
                    n.nin                            AS "nin?",
                    n.first_name                     AS "nin_first_name?",
                    n.middle_name                    AS "nin_middle_name?",
                    n.last_name                      AS "nin_last_name?",
                    n.gender                         AS "nin_gender?",
                    n.date_of_birth                  AS "nin_dob?",
                    n.phone_number                   AS "nin_phone?",
                    b.bvn                            AS "bvn?",
                    b.first_name                     AS "bvn_first_name?",
                    b.middle_name                    AS "bvn_middle_name?",
                    b.last_name                      AS "bvn_last_name?",
                    b.gender                         AS "bvn_gender?",
                    b.date_of_birth                  AS "bvn_dob?",
                    b.phone_number                   AS "bvn_phone?"
                FROM kyc_verifications kv
                LEFT JOIN kyc_nin_data n ON n.kyc_verification_id = kv.id
                LEFT JOIN kyc_bvn_data b ON b.kyc_verification_id = kv.id
                WHERE kv.reference_id = $1
                ORDER BY kv.created_at DESC
                LIMIT 1
                "#,
                kyc_reference
            )
            .fetch_optional(db.get_ref())
            .await
            {
                Ok(Some(identity)) => {
                    let parsed = match identity.id_type.as_str() {
                        "NIN" => identity.nin.clone().map(|nin| {
                            (
                                "NIN".to_string(),
                                nin,
                                identity.nin_first_name.clone().unwrap_or_default(),
                                identity.nin_middle_name.clone().unwrap_or_default(),
                                identity.nin_last_name.clone().unwrap_or_default(),
                                identity.nin_gender.clone().unwrap_or_default(),
                                identity
                                    .nin_dob
                                    .map(|d| d.format("%Y-%m-%d").to_string())
                                    .unwrap_or_default(),
                                identity.nin_phone.clone().unwrap_or_default(),
                            )
                        }),
                        "BVN" => identity.bvn.clone().map(|bvn| {
                            (
                                "BVN".to_string(),
                                bvn,
                                identity.bvn_first_name.clone().unwrap_or_default(),
                                identity.bvn_middle_name.clone().unwrap_or_default(),
                                identity.bvn_last_name.clone().unwrap_or_default(),
                                identity.bvn_gender.clone().unwrap_or_default(),
                                identity
                                    .bvn_dob
                                    .map(|d| d.format("%Y-%m-%d").to_string())
                                    .unwrap_or_default(),
                                identity.bvn_phone.clone().unwrap_or_default(),
                            )
                        }),
                        other => {
                            println!(
                                "[verify_otp] id_type '{}' not NIN or BVN for reference={}",
                                other, kyc_reference
                            );
                            None
                        }
                    };

                    if let Some((
                        id_type,
                        id_value,
                        first_name,
                        middle_name,
                        last_name,
                        gender,
                        dob,
                        phone,
                    )) = parsed
                    {
                        if id_value.is_empty() {
                            println!(
                                "[verify_otp] id_value empty for reference={}",
                                kyc_reference
                            );
                        } else {
                            let gender_code = match gender.to_uppercase().as_str() {
                                "MALE" => 0,
                                "FEMALE" => 1,
                                _ => 0,
                            };

                            let other_names = format!("{} {}", first_name, middle_name);

                            let date_of_birth_fmt = {
                                let parts: Vec<&str> = dob.split('-').collect();
                                if parts.len() == 3 {
                                    format!("{}/{}/{}", parts[2], parts[1], parts[0])
                                } else {
                                    dob.clone()
                                }
                            };

                            let transaction_ref = uuid::Uuid::new_v4().to_string();
                            let address = account.address.clone().unwrap_or_default();

                            let mut open_wallet_body = serde_json::json!({
                                "transactionTrackingRef": transaction_ref,
                                "lastName":               last_name,
                                "otherNames":             other_names,
                                "phoneNo":                phone,
                                "gender":                 gender_code,
                                "dateOfBirth":            date_of_birth_fmt,
                                "address":                address,
                                "email":                  account.email,
                            });

                            if id_type == "NIN" {
                                open_wallet_body["nationalIdentityNo"] =
                                    serde_json::json!(id_value);
                            } else {
                                open_wallet_body["bvn"] = serde_json::json!(id_value);
                            }

                            println!(
                                "[verify_otp] 9PSB request body: {}",
                                serde_json::to_string_pretty(&open_wallet_body).unwrap_or_default()
                            );

                            match PsbClient::new(cfg.get_ref())
                                .post(
                                    &mut redis_conn,
                                    "/waas/api/v1/open_wallet",
                                    &open_wallet_body,
                                )
                                .await
                            {
                                Ok(json) => {
                                    let status_str =
                                        json["status"].as_str().unwrap_or("").to_uppercase();

                                    if status_str != "SUCCESS" {
                                        println!(
                                            "[verify_otp] 9PSB rejected — status: '{}', message: '{}'",
                                            status_str,
                                            json["message"].as_str().unwrap_or("no message")
                                        );
                                    } else {
                                        let account_number = json["data"]["accountNumber"]
                                            .as_str()
                                            .unwrap_or("")
                                            .to_string();
                                        let account_name = json["data"]["fullName"]
                                            .as_str()
                                            .unwrap_or("")
                                            .to_string();

                                        if account_number.is_empty() {
                                            println!(
                                                "[verify_otp] accountNumber empty in 9PSB response"
                                            );
                                        } else {
                                            let nin_val = if id_type == "NIN" {
                                                Some(id_value.clone())
                                            } else {
                                                None
                                            };
                                            let bvn_val = if id_type == "BVN" {
                                                Some(id_value.clone())
                                            } else {
                                                None
                                            };

                                            if let Err(e) = sqlx::query!(
                                                r#"UPDATE accounts SET
                                                    account_number   = $1,
                                                    account_name     = $2,
                                                    bank_name        = '9PSB',
                                                    firstname        = COALESCE(firstname, $3),
                                                    lastname         = COALESCE(lastname, $4),
                                                    othername        = COALESCE(othername, $5),
                                                    phone_number     = COALESCE(phone_number, $6),
                                                    gender           = COALESCE(gender, $7),
                                                    date_of_birth    = COALESCE(date_of_birth, $8),
                                                    nin              = COALESCE(nin, $9),
                                                    bvn              = COALESCE(bvn, $10),
                                                    current_tier     = GREATEST(current_tier, 1),
                                                    tier_upgraded_at = NOW(),
                                                    updated_at       = NOW()
                                                WHERE id = $11"#,
                                                account_number,
                                                account_name,
                                                first_name,
                                                last_name,
                                                middle_name,
                                                phone,
                                                gender,
                                                dob,
                                                nin_val,
                                                bvn_val,
                                                account.id
                                            )
                                            .execute(db.get_ref())
                                            .await
                                            {
                                                println!(
                                                    "[verify_otp] failed to persist wallet info: {}",
                                                    e
                                                );
                                            } else {
                                                println!(
                                                    "[verify_otp] 9PSB wallet opened — email: {}, account_number: {}",
                                                    email, account_number
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("[verify_otp] 9PSB wallet open failed: {}", e);
                                }
                            }
                        }
                    } else {
                        println!(
                            "[verify_otp] identity data missing for id_type={} reference={}",
                            identity.id_type, kyc_reference
                        );
                    }
                }
                Ok(None) => {
                    println!(
                        "[verify_otp] no kyc_verifications row for reference={}",
                        kyc_reference
                    );
                }
                Err(e) => {
                    println!("[verify_otp] kyc_verifications fetch error: {}", e);
                }
            }
        } else {
            println!(
                "[verify_otp] account {} has no kyc_reference — skipping wallet resolution",
                email
            );
        }
    }

    let secret = cfg.jwt_secret.clone();
    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) }
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
            eprintln!("[signin] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let password_hash = row.password_hash.clone();
    let input_password = body.password.clone();

    let is_valid =
        tokio::task::spawn_blocking(move || verify_password(&input_password, &password_hash))
            .await
            .unwrap_or(Ok(false));

    if !matches!(is_valid, Ok(true)) {
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

        if current_trials % TRIALS_PER_BLOCK == 0 {
            let block = (current_trials / TRIALS_PER_BLOCK - 1)
                .min((LOCKOUT_SEQUENCE.len() as i64) - 1) as usize;
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

    let _: Result<(), _> = redis::cmd("DEL")
        .arg(&[&lockout_key, &trials_key, &block_key])
        .query_async(&mut redis_conn)
        .await;

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

    if row.is_2fa_enabled {
        return HttpResponse::Ok().json(serde_json::json!({
            "status": "success",
            "message": "2FA verification required.",
            "requires_2fa": true,
        }));
    }

    let event = AccountLoggedInNotificationEvent {
        email: email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
        ip: ip.clone(),
    };
    kafka.publish(&kafka_cfg.kafka_account_login_successful, &email, &event);

    let account_id = row.id.to_string();
    let secret = cfg.jwt_secret.clone();

    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) }
    );

    let access_token = match access_token {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[signin] Access Token error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let refresh_token = match refresh_token {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[signin] Refresh Token error: {}", e);
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

    if stored_otp != body.otp {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Invalid or expired OTP".into(),
            status: ResponseStatus::ERROR,
        });
    }

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

    let account_id = row.id.to_string();
    let secret = cfg.jwt_secret.clone();

    let (access_token, refresh_token) = tokio::join!(
        async { generate_access_token(&account_id, &secret) },
        async { generate_refresh_token(&account_id, &secret) }
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

    let claims = match verify_token(&body.refresh_token, &cfg.jwt_secret) {
        Ok(c) => c,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid or expired refresh token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    if claims.token_type != "refresh" {
        return HttpResponse::Unauthorized().json(ApiResponse {
            message: "Invalid token type".into(),
            status: ResponseStatus::ERROR,
        });
    }

    let secret = cfg.jwt_secret.clone();
    let id = claims.sub.clone();

    let (access_token, refresh_token) =
        tokio::join!(async { generate_access_token(&id, &secret) }, async {
            generate_refresh_token(&id, &secret)
        });

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

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let (account, contact_history): (Result<_, sqlx::Error>, Result<_, sqlx::Error>) = tokio::join!(
        sqlx::query!(
            r#"SELECT id, email, firstname, lastname, othername, phone_number,
               email_verified, phone_no_verified, status, account_type,
               is_2fa_enabled, user_photo, device_id, current_tier, pending_tier_upgrade,
               tier_upgraded_at, tier_upgrade_requested_at, panic_enabled,
               panic_message, panic_activated_at, panic_deactivated_at,
               account_number, account_name, bvn, nin,
               nin_userid, created_at, updated_at, referral_code
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

    let today = chrono::Utc::now().format("%Y-%m-%d");
    let daily_key = format!("daily_txn_total:{}:{}", account_uuid, today);

    let daily_total: f64 = redis::cmd("GET")
        .arg(&daily_key)
        .query_async(&mut redis_conn)
        .await
        .unwrap_or(None)
        .unwrap_or(0.0);

    let daily_limit: Option<f64> = match row.current_tier {
        1 => Some(50_000.0),
        2 => Some(200_000.0),
        _ => None, // tier 3 = unlimited
    };

    let exceeded_limit = daily_limit.map(|limit| daily_total >= limit);

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
        "user_photo": row.user_photo,
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
        "referral_code": row.referral_code,
        "contact_history": history,
        "daily_outgoing_total": daily_total,
        "daily_limit":          daily_limit,
        "exceeded_limit":       exceeded_limit,
    });

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

pub async fn verify_palmpayment(
    auth: AuthUser,
    cfg: web::Data<crate::config::Config>,
    client: web::Data<reqwest::Client>,
    db: web::Data<PgPool>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    let request_id = uuid::Uuid::new_v4();

    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let payload = body.into_inner();
    let target_url = format!("{}/verify-payment", cfg.palm_api_url);

    let res = client
        .post(&target_url)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();

            match response.json::<serde_json::Value>().await {
                Ok(body) => {
                    // success=true + verified=true → money moved, advance referral
                    // success=true + verified=false → panic / mismatch, no update
                    // 202 pending → success=false, excluded correctly
                    let palm_success = body["success"].as_bool() == Some(true)
                        && body["verified"].as_bool() == Some(true);

                    if palm_success {
                        let db_clone = db.clone();
                        tokio::spawn(async move {
                            match
                                sqlx
                                    ::query!(
                                        r#"
                                INSERT INTO referrals (
                                    referrer_id,
                                    referred_id,
                                    status,
                                    palm_transfer_at
                                )
                                SELECT
                                    a.referred_by,
                                    a.id,
                                    'palm_done',
                                    NOW()
                                FROM accounts a
                                WHERE a.id          = $1
                                  AND a.referred_by IS NOT NULL
                                ON CONFLICT (referred_id) DO UPDATE
                                    SET status           = CASE
                                                               WHEN referrals.status = 'pending'
                                                                    AND referrals.palm_transfer_at IS NULL
                                                               THEN 'palm_done'
                                                               ELSE referrals.status
                                                           END,
                                        palm_transfer_at = CASE
                                                               WHEN referrals.palm_transfer_at IS NULL
                                                               THEN NOW()
                                                               ELSE referrals.palm_transfer_at
                                                           END,
                                        updated_at       = NOW()
                                WHERE referrals.palm_transfer_at IS NULL
                                  AND referrals.status = 'pending'
                                "#,
                                        account_uuid
                                    )
                                    .execute(db_clone.get_ref()).await
                            {
                                Ok(r) => {
                                    if r.rows_affected() > 0 {
                                        log::info!(
                                            "[referral] palm_done set for referred_id={}",
                                            account_uuid
                                        );
                                    }
                                    // rows_affected == 0 means either:
                                    // - user was not referred (referred_by IS NULL)
                                    // - palm already recorded (palm_transfer_at IS NOT NULL)
                                    // - user was disqualified (vas before palm) — correctly ignored
                                    // all are correct, nothing to do
                                }
                                Err(e) => {
                                    log::error!(
                                        "[referral] palm_done upsert failed for referred_id={}: {}",
                                        account_uuid,
                                        e
                                    );
                                }
                            }
                        });
                    }

                    HttpResponse::build(
                        actix_web::http::StatusCode::from_u16(status.as_u16())
                            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                    )
                    .json(body)
                }

                Err(e) => {
                    log::error!("[{request_id}] failed to parse palm response: {e}");
                    HttpResponse::InternalServerError().json(ApiResponse {
                        message: "Failed to parse palm service response".into(),
                        status: ResponseStatus::ERROR,
                    })
                }
            }
        }

        Err(e) => {
            log::error!("[{request_id}] palm service unreachable: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn enroll_palm(
    auth: AuthUser,
    cfg: web::Data<crate::config::Config>,
    db: web::Data<PgPool>,
    body: web::Json<serde_json::Value>,
) -> impl Responder {
    if body.get("type").and_then(|t| t.as_str()) == Some("panic") {
        let enabled = body
            .get("enabled")
            .and_then(|e| e.as_bool())
            .unwrap_or(false);

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
                body.get("message")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Service temporarily unavailable")
                    .to_string(),
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

        if let Err(e) = result {
            log::error!("Failed to update panic mode: {e}");
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Failed to update panic mode".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let client = reqwest::Client::new();

    let res = client
        .post(format!("{}/enroll", cfg.palm_api_url))
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .json(&body.into_inner())
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                )
                .json(body),
                Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Failed to parse palm service response".into(),
                    status: ResponseStatus::ERROR,
                }),
            }
        }
        Err(e) => {
            log::error!("Palm enroll request failed: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn delete_palm(
    auth: AuthUser,
    cfg: web::Data<crate::config::Config>,
    path: web::Path<String>,
) -> impl Responder {
    let palm_type = path.into_inner();

    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let client = reqwest::Client::new();

    let res = client
        .delete(format!(
            "{}/palm/enrollment/{}",
            cfg.palm_api_url, palm_type
        ))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                )
                .json(body),
                Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Failed to parse palm service response".into(),
                    status: ResponseStatus::ERROR,
                }),
            }
        }
        Err(e) => {
            log::error!("Palm delete request failed: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn fetch_supported_countries() -> impl Responder {
    let countries: Vec<Country> = vec![
        // ── Explicitly supported ──
        Country {
            name: "Ghana",
            short_name: "GH",
            symbol: "₵",
            currency_code: "GHS",
            country_code: "+233",
        },
        Country {
            name: "Senegal",
            short_name: "SN",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+221",
        },
        Country {
            name: "Tanzania",
            short_name: "TZ",
            symbol: "TSh",
            currency_code: "TZS",
            country_code: "+255",
        },
        Country {
            name: "Kenya",
            short_name: "KE",
            symbol: "KSh",
            currency_code: "KES",
            country_code: "+254",
        },
        Country {
            name: "Cameroon",
            short_name: "CM",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+237",
        },
        Country {
            name: "Ethiopia",
            short_name: "ET",
            symbol: "Br",
            currency_code: "ETB",
            country_code: "+251",
        },
        Country {
            name: "Rwanda",
            short_name: "RW",
            symbol: "Fr",
            currency_code: "RWF",
            country_code: "+250",
        },
        Country {
            name: "Zambia",
            short_name: "ZM",
            symbol: "ZK",
            currency_code: "ZMW",
            country_code: "+260",
        },
        Country {
            name: "Cote d'Ivoire",
            short_name: "CI",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+225",
        },
        // ── Francophone Africa ──
        Country {
            name: "Mali",
            short_name: "ML",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+223",
        },
        Country {
            name: "Burkina Faso",
            short_name: "BF",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+226",
        },
        Country {
            name: "Niger",
            short_name: "NE",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+227",
        },
        Country {
            name: "Guinea",
            short_name: "GN",
            symbol: "Fr",
            currency_code: "GNF",
            country_code: "+224",
        },
        Country {
            name: "Benin",
            short_name: "BJ",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+229",
        },
        Country {
            name: "Togo",
            short_name: "TG",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+228",
        },
        Country {
            name: "Chad",
            short_name: "TD",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+235",
        },
        Country {
            name: "Central African Republic",
            short_name: "CF",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+236",
        },
        Country {
            name: "Republic of the Congo",
            short_name: "CG",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+242",
        },
        Country {
            name: "Democratic Republic of the Congo",
            short_name: "CD",
            symbol: "FC",
            currency_code: "CDF",
            country_code: "+243",
        },
        Country {
            name: "Gabon",
            short_name: "GA",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+241",
        },
        Country {
            name: "Equatorial Guinea",
            short_name: "GQ",
            symbol: "CFA",
            currency_code: "XAF",
            country_code: "+240",
        },
        Country {
            name: "Madagascar",
            short_name: "MG",
            symbol: "Ar",
            currency_code: "MGA",
            country_code: "+261",
        },
        Country {
            name: "Mauritius",
            short_name: "MU",
            symbol: "₨",
            currency_code: "MUR",
            country_code: "+230",
        },
        Country {
            name: "Seychelles",
            short_name: "SC",
            symbol: "₨",
            currency_code: "SCR",
            country_code: "+248",
        },
        Country {
            name: "Comoros",
            short_name: "KM",
            symbol: "Fr",
            currency_code: "KMF",
            country_code: "+269",
        },
        Country {
            name: "Djibouti",
            short_name: "DJ",
            symbol: "Fr",
            currency_code: "DJF",
            country_code: "+253",
        },
        Country {
            name: "Burundi",
            short_name: "BI",
            symbol: "Fr",
            currency_code: "BIF",
            country_code: "+257",
        },
        Country {
            name: "Guinea-Bissau",
            short_name: "GW",
            symbol: "CFA",
            currency_code: "XOF",
            country_code: "+245",
        },
        Country {
            name: "Mauritania",
            short_name: "MR",
            symbol: "UM",
            currency_code: "MRU",
            country_code: "+222",
        },
        Country {
            name: "Morocco",
            short_name: "MA",
            symbol: "د.م.",
            currency_code: "MAD",
            country_code: "+212",
        },
        Country {
            name: "Algeria",
            short_name: "DZ",
            symbol: "د.ج",
            currency_code: "DZD",
            country_code: "+213",
        },
        Country {
            name: "Tunisia",
            short_name: "TN",
            symbol: "د.ت",
            currency_code: "TND",
            country_code: "+216",
        },
    ];

    HttpResponse::Ok().json(countries)
}

pub async fn get_palm(auth: AuthUser, cfg: web::Data<crate::config::Config>) -> impl Responder {
    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/palm", cfg.palm_api_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                )
                .json(body),
                Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Failed to parse palm service response".into(),
                    status: ResponseStatus::ERROR,
                }),
            }
        }
        Err(e) => {
            log::error!("Palm service request failed: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn get_panic_status(
    auth: AuthUser,
    cfg: web::Data<crate::config::Config>,
) -> impl Responder {
    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/palm/panic/status", cfg.palm_api_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                )
                .json(body),
                Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Failed to parse palm service response".into(),
                    status: ResponseStatus::ERROR,
                }),
            }
        }
        Err(e) => {
            log::error!("Palm service request failed: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn revoke_panic(auth: AuthUser, cfg: web::Data<crate::config::Config>) -> impl Responder {
    let token = match auth.token {
        Some(t) => t,
        None => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Missing token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let client = reqwest::Client::new();

    let res = client
        .patch(format!("{}/palm/panic/revoke", cfg.palm_api_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await;

    match res {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(body) => HttpResponse::build(
                    actix_web::http::StatusCode::from_u16(status.as_u16())
                        .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR),
                )
                .json(body),
                Err(_) => HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Failed to parse palm service response".into(),
                    status: ResponseStatus::ERROR,
                }),
            }
        }
        Err(e) => {
            log::error!("Palm service request failed: {e}");
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
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

    let event = ChangePasswordEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        new_password: body.new_password.clone(),
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

    let event = ChangePasswordEvent {
        account_id: row.id.to_string(),
        email: email.clone(),
        new_password: body.new_password.clone(),
    };

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

    if old_email == new_email {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "New email address must be different from your current email.".into(),
            status: ResponseStatus::ERROR,
        });
    }

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
            println!(
                "[change_email_submit] DB error checking email uniqueness: {}",
                e
            );
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {}
    }

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

    // ── Fetch BVN from kyc_verifications using the reference ──────────────────
    let bvn: String = match sqlx::query_scalar!(
        r#"
        SELECT value FROM kyc_verifications
        WHERE reference_id = $1
          AND status = true
          AND verification_type = 'bvn'
        LIMIT 1
        "#,
        body.reference
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(Some(v))) => v,
        Ok(Some(None)) | Ok(None) => {
            return HttpResponse::BadRequest().json(
                serde_json::json!({
                "status": "error",
                "message": "Invalid or expired BVN verification reference. Please complete BVN verification first."
            })
            );
        }
        Err(e) => {
            eprintln!("[upgrade_tier2] KYC lookup error: {:?}", e);
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": "Something went wrong. Please try again."
            }));
        }
    };

    // ── Fetch account fields ──────────────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT nin, user_photo, phone_number, current_tier, pending_tier_upgrade, status
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

    // ── Publish tier 2 upgrade event ──────────────────────────────────────────
    let event = Tier2UpgradeEvent {
        account_id: account_id.clone(),
        bvn,
        phone_no: row.phone_number.clone().unwrap_or_default(),
        id_type: 1,
        id_number: row.nin.clone().unwrap_or_default(),
        nin: row.nin.clone().unwrap_or_default(),
        house_number: body.house_number.clone(),
        street_name: body.street_name.clone(),
        state: body.state.clone(),
        city: body.city.clone(),
        local_government: body.local_government.clone(),
        nearest_landmark: body.nearest_landmark.clone(),
        pep,
        user_photo: row.user_photo.clone().unwrap_or_default(),
        id_card_front: body.id_card_front.clone(),
        customer_signature: body.customer_signature.clone(),
        utility_bill: body.utility_bill.clone(),
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
    cfg: web::Data<Config>,
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
        let results = sqlx::query!(
            r#"
            SELECT
                id, firstname, lastname, othername, phone_number,
                email, account_number, account_name, account_type, current_tier
            FROM accounts
            WHERE
                deleted_at IS NULL
                AND (
                    TRIM(account_number::TEXT) = $1
                    OR TRIM(phone_number) = $1
                    OR LOWER(TRIM(email)) = $1
                    OR LOWER(SPLIT_PART(account_name, '/', 2)) LIKE $2
                    OR LOWER(account_name) LIKE $2
                    OR LOWER(firstname) LIKE $2
                    OR LOWER(lastname) LIKE $2
                )
            LIMIT 11
            "#,
            q,
            like_q
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
                        .filter(|s: &&&str| !s.is_empty())
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

                if !all.is_empty() {
                    let _: Result<(), _> = redis::cmd("SETEX")
                        .arg(&cache_key)
                        .arg(60u64)
                        .arg(serde_json::to_string(&all).unwrap_or_default())
                        .query_async::<_, ()>(&mut redis_conn)
                        .await;
                }

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

    let mut redis_conn_psb = redis.get_ref().clone();
    let mut enriched: Vec<serde_json::Value> = Vec::with_capacity(accounts.len());

    for mut account in accounts {
        let account_number = match account["account_number"].as_str() {
            Some(n) if !n.is_empty() => n.to_string(),
            _ => {
                enriched.push(account);
                continue;
            }
        };

        match PsbClient::new(&cfg)
            .post(
                &mut redis_conn_psb,
                "/waas/api/v1/wallet_enquiry",
                &serde_json::json!({ "accountNo": account_number }),
            )
            .await
        {
            Ok(response) => {
                let status = response["status"].as_str().unwrap_or("").to_uppercase();
                if status == "SUCCESS" {
                    let wallet_data = &response["data"];

                    let tier = wallet_data["tier"]
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

                    let mut wallet = wallet_data.clone();
                    wallet["limits"] = limits;
                    let account_id_str = account["id"].as_str().unwrap_or("");

                    if !account_id_str.is_empty() {
                        let today = chrono::Utc::now().format("%Y-%m-%d");
                        let daily_key = format!("daily_txn_total:{}:{}", account_id_str, today);
                        let mut redis_daily = redis.get_ref().clone();

                        let daily_total: f64 = redis::cmd("GET")
                            .arg(&daily_key)
                            .query_async(&mut redis_daily)
                            .await
                            .unwrap_or(None)
                            .unwrap_or(0.0);

                        let daily_limit: Option<f64> = match tier {
                            1 => Some(50_000.0),
                            2 => Some(200_000.0),
                            _ => None,
                        };

                        let exceeded_limit = daily_limit.map(|limit| daily_total >= limit);

                        wallet["daily_outgoing_total"] = serde_json::json!(daily_total);
                        wallet["daily_limit"] = serde_json::json!(daily_limit);
                        wallet["exceeded_limit"] = serde_json::json!(exceeded_limit);
                    }

                    account["wallet"] = wallet;
                }
            }
            Err(e) => {
                log::warn!(
                    "[search_account] PSB wallet_enquiry failed for {}: {}",
                    account_number,
                    e
                );
            }
        }

        enriched.push(account);
    }

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data": enriched,
    }))
}

pub async fn set_payment_pin(
    auth: AuthUser,
    body: web::Json<schemas::SetPaymentPinRequest>,
    db: web::Data<PgPool>,
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
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[set_payment_pin] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    let password_hash = row.password_hash.clone();
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
                status: ResponseStatus::ERROR,
            });
        }
    }

    let plain_pin = body.new_pin.clone();

    let pin_hash = match tokio::task::spawn_blocking(move || hash_password(&plain_pin)).await {
        Ok(Ok(h)) => h,
        _ => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

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
            status: ResponseStatus::ERROR,
        });
    }

    let event = SetPaymentPinEvent {
        account_id: auth.id.clone(),
        email: row.email.clone(),
        firstname: row.firstname.clone().unwrap_or_default(),
    };

    kafka.publish(&kafka_cfg.kafka_topic_payment_pin_set, &row.email, &event);

    let mut redis_conn = redis.get_ref().clone();
    let account_cache_key = format!("account:{}", auth.id);
    let pin_hash_cache_key = format!("pin_hash:{}", account_uuid);

    let _: Result<(), _> = redis::cmd("DEL")
        .arg(&account_cache_key)
        .arg(&pin_hash_cache_key)
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(ApiResponse {
        message: "Payment PIN set successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn update_device_token(
    auth: AuthUser,
    body: web::Json<UpdateDeviceTokenRequest>,
    db: web::Data<PgPool>,
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

    match
        sqlx
            ::query!(
                "UPDATE accounts SET device_token = $1, updated_at = NOW() WHERE id = $2 AND deleted_at IS NULL",
                body.device_token,
                account_uuid
            )
            .execute(db.get_ref()).await
    {
        Ok(_) =>
            HttpResponse::Ok().json(ApiResponse {
                message: "Device token updated".into(),
                status: ResponseStatus::SUCCESS,
            }),
        Err(e) => {
            log::error!("[update_device_token] DB error: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn wallet_enquiry(
    auth: AuthUser,
    db: web::Data<PgPool>,
    cfg: web::Data<Config>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let account_uuid = match Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

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
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            log::error!("[wallet_enquiry] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    match row.status.as_str() {
        "active" => {}
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
        _ => {
            return HttpResponse::Forbidden().json(ApiResponse {
                message: "Account is not active.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    let account_number = match row.account_number {
        Some(n) => n,
        None => {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "Your wallet is not yet activated. Please complete KYC to proceed.".into(),
                status: ResponseStatus::ERROR,
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
                status: ResponseStatus::ERROR,
            });
        }
    };

    let status = response["status"].as_str().unwrap_or("").to_uppercase();
    if status != "SUCCESS" {
        return HttpResponse::BadRequest().json(ApiResponse {
            message: "Unable to retrieve wallet information.".into(),
            status: ResponseStatus::ERROR,
        });
    }

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

    let mut data = response["data"].clone();
    data["limits"] = limits;

    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "data":   data,
    }))
}

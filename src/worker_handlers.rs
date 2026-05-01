use crate::config::Config;
use crate::modules::account::schemas::Gender;
use crate::modules::legacy_plan::events::{
    LegacyPlanBeneficiaryAddedEvent, LegacyPlanBeneficiaryDeletedEvent,
};
use crate::modules::transactions::event::{
    ExternalTransferInitiatedEvent, InternalTransferInitiatedEvent,
};
use crate::modules::vas::events::{AirtimePurchaseEvent, DataPurchaseEvent};
use crate::utils::fms::send_push_notification;
use crate::utils::mailer::Mailer;
use crate::utils::password_manager::hash_password;
use crate::utils::psb::PsbClient;
use crate::utils::vas::VasClient;
use crate::worker_events::{
    AccountLoggedInNotificationEvent, ChangeEmailEvent, ChangePasswordEvent, DeleteAccountEvent,
    InboundTransferEvent, KycUpgradeStatusEvent, NewDeviceLoginEvent, OpenWalletEvent,
    SetPaymentPinEvent, SignupEvent, SuspiciousLoginEvent, Tier2UpgradeEvent, Tier3UpgradeEvent,
    UpdateDeviceIdEvent, VerifyEmailEvent,
};
use rand::Rng;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn handle_signup(
    payload: &str,
    db: &PgPool,
    redis: &redis::aio::ConnectionManager,
    mailer: &Mailer,
) {
    let event: SignupEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/signup] Invalid payload: {}", e);
            return;
        }
    };

    println!("[worker/signup] Processing: {}", event.email);

    // ── 1. Check if email already exists ─────────────────────────────────────
    let existing = sqlx::query!(
        "SELECT id, email_verified FROM accounts WHERE email = $1",
        event.email
    )
    .fetch_optional(db)
    .await;

    match existing {
        Ok(Some(row)) => {
            if row.email_verified {
                println!(
                    "[worker/signup] Email already verified, rejecting: {}",
                    event.email
                );
                return;
            }
            println!(
                "[worker/signup] Email exists but unverified, resending OTP: {}",
                event.email
            );
        }
        Err(e) => {
            println!("[worker/signup] DB check error: {}", e);
            return;
        }
        Ok(None) => {
            // ── 2. Hash password ──────────────────────────────────────────────
            let password_hash = match hash_password(&event.password) {
                Ok(h) => h,
                Err(e) => {
                    println!("[worker/signup] Hash error: {}", e);
                    return;
                }
            };

            // ── 3. Create account in DB ───────────────────────────────────────
            let account_id = Uuid::new_v4();
            let result = sqlx::query!(
                r#"
                INSERT INTO accounts (id, email, password_hash, device_id, account_type, status, email_verified)
                VALUES ($1, $2, $3, $4, $5, 'active', false)
                "#,
                account_id,
                event.email,
                password_hash,
                event.device_id,
                event.account_type,
            )
            .execute(db)
            .await;

            if let Err(e) = result {
                println!("[worker/signup] DB insert error: {}", e);
                return;
            }

            println!("[worker/signup] Account created: {}", event.email);
        }
    }

    // ── 4. Generate + hash OTP ────────────────────────────────────────────────
    let otp = generate_otp();

    let otp_hash = match hash_password(&otp) {
        Ok(h) => h,
        Err(e) => {
            println!("[worker/signup] OTP hash error: {}", e);
            return;
        }
    };

    // ── 5. Store hashed OTP in Redis — TTL 10 minutes ─────────────────────────
    let mut redis_conn = redis.clone();
    match redis::cmd("SETEX")
        .arg(&event.otp_redis_key)
        .arg(600u64)
        .arg(&otp_hash)
        .query_async::<_, ()>(&mut redis_conn)
        .await
    {
        Ok(_) => println!("[worker/signup] OTP stored: {}", event.otp_redis_key),
        Err(e) => println!("[worker/signup] Redis store error: {}", e),
    }

    // ── 6. Send plain OTP in email ────────────────────────────────────────────
    if let Err(e) = mailer.send_otp(&event.email, "", &otp).await {
        println!("[worker/signup] OTP email error: {}", e);
    }

    println!("[worker/signup] Done: {}", event.email);
}

pub async fn handle_resend_otp(
    payload: &str,
    redis: &redis::aio::ConnectionManager,
    mailer: &Mailer,
) {
    #[derive(Deserialize)]
    struct ResendOtpEvent {
        email: String,
        otp_redis_key: String,
    }

    let event: ResendOtpEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/resend_otp] Invalid payload: {}", e);
            return;
        }
    };

    let otp = generate_otp();

    let otp_hash = match hash_password(&otp) {
        Ok(h) => h,
        Err(e) => {
            println!("[worker/resend_otp] OTP hash error: {}", e);
            return;
        }
    };

    let mut redis_conn = redis.clone();
    match redis::cmd("SETEX")
        .arg(&event.otp_redis_key)
        .arg(600u64)
        .arg(&otp_hash)
        .query_async::<_, ()>(&mut redis_conn)
        .await
    {
        Ok(_) => println!("[worker/resend_otp] OTP stored: {}", event.otp_redis_key),
        Err(e) => println!("[worker/resend_otp] Redis store error: {}", e),
    }

    if let Err(e) = mailer.send_otp(&event.email, "", &otp).await {
        println!("[worker/resend_otp] Email error: {}", e);
    }

    println!("[worker/resend_otp] OTP resent: {}", event.email);
}

pub async fn handle_verify_email(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: VerifyEmailEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/verify_email] Invalid payload: {}", e);
            return;
        }
    };

    match sqlx::query!(
        "UPDATE accounts SET email_verified = true WHERE email = $1",
        event.email
    )
    .execute(db)
    .await
    {
        Ok(_) => {
            println!("[worker/verify_email] Email verified: {}", event.email);

            // ── Fire welcome email ────────────────────────────────────────────
            if let Err(e) = mailer.send_welcome(&event.email, "").await {
                println!("[worker/verify_email] Welcome email error: {}", e);
            }
        }
        Err(e) => println!("[worker/verify_email] DB error: {}", e),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn generate_otp() -> String {
    let mut rng = rand::thread_rng();
    format!("{:06}", rng.gen_range(100000..999999))
}

pub async fn handle_change_password(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: ChangePasswordEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/change_password] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/change_password] Invalid account_id");
            return;
        }
    };

    // ── Hash new password ─────────────────────────────────────────────────────
    let new_hash = match hash_password(&event.new_password) {
        Ok(h) => h,
        Err(e) => {
            println!("[worker/change_password] Hash error: {}", e);
            return;
        }
    };

    // ── Update password + insert contact history concurrently ─────────────────
    let (update, history): (Result<_, sqlx::Error>, Result<_, sqlx::Error>) = tokio::join!(
        sqlx::query!(
            "UPDATE accounts SET password_hash = $1, updated_at = NOW() WHERE id = $2",
            new_hash,
            account_uuid
        )
        .execute(db),
        sqlx::query!(
            r#"INSERT INTO account_contact_history
               (account_id, field, old_value, new_value, changed_at)
               VALUES ($1, 'password', '[hashed]', '[hashed]', NOW())
               ON CONFLICT (account_id, field, old_value, new_value) DO NOTHING"#,
            account_uuid,
        )
        .execute(db)
    );

    if let Err(e) = update {
        println!("[worker/change_password] DB update error: {}", e);
        return;
    }

    if let Err(e) = history {
        println!("[worker/change_password] History insert error: {}", e);
    }

    // ── Send notification email ───────────────────────────────────────────────
    if let Err(e) = mailer.send_password_changed(&event.email, "").await {
        println!("[worker/change_password] Email error: {}", e);
    }

    println!("[worker/change_password] Done: {}", event.email);
}

pub async fn handle_delete_account(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: DeleteAccountEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/delete_account] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/delete_account] Invalid account_id");
            return;
        }
    };

    // ── Soft delete account ───────────────────────────────────────────────────
    let update = sqlx::query!(
        r#"UPDATE accounts
           SET deleted_at = NOW(),
               deleted_reason = $1,
               status = 'deleted',
               updated_at = NOW()
           WHERE id = $2 AND deleted_at IS NULL"#,
        event.deletion_reason,
        account_uuid,
    )
    .execute(db)
    .await;

    if let Err(e) = update {
        println!("[worker/delete_account] DB error: {}", e);
        return;
    }

    // ── Send deletion confirmation email ──────────────────────────────────────
    if let Err(e) = mailer
        .send_account_deleted(&event.email, "", event.deletion_reason)
        .await
    {
        println!("[worker/delete_account] Email error: {}", e);
    }

    println!("[worker/delete_account] Done: {}", event.email);
}

pub async fn handle_suspicious_login(payload: &str, mailer: &Mailer) {
    let event: SuspiciousLoginEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/suspicious_login] Invalid payload: {}", e);
            return;
        }
    };

    if let Err(e) = mailer
        .send_suspicious_login(&event.email, &event.firstname, &event.ip)
        .await
    {
        println!("[worker/suspicious_login] Email error: {}", e);
    }

    println!("[worker/suspicious_login] Done: {}", event.email);
}

pub async fn handle_new_device_login(payload: &str, mailer: &Mailer) {
    let event: NewDeviceLoginEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/new_device_login] Invalid payload: {}", e);
            return;
        }
    };

    println!(
        "[worker/new_device_login] email: {}, otp: {}, ip: {}, firstname: {}",
        event.email, event.otp, event.ip, event.firstname
    );

    if let Err(e) = mailer
        .send_new_device_otp(&event.email, &event.firstname, &event.otp, &event.ip)
        .await
    {
        println!("[worker/new_device_login] Email error: {:#?}", e);
    }

    println!("[worker/new_device_login] Done: {}", event.email);
}

pub async fn handle_device_update(payload: &str, db: &PgPool) {
    let event: UpdateDeviceIdEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/device_update] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/device_update] Invalid account_id");
            return;
        }
    };

    let result = sqlx::query!(
        "UPDATE accounts SET device_id = $1, updated_at = NOW() WHERE id = $2",
        event.new_device_id,
        account_uuid,
    )
    .execute(db)
    .await;

    match result {
        Ok(_) => println!("[worker/device_update] Done: {}", event.email),
        Err(e) => println!("[worker/device_update] DB error: {}", e),
    }
}

pub async fn handle_account_loggedin_notification(payload: &str, mailer: &Mailer) {
    let event: AccountLoggedInNotificationEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/logged_in_notification] Invalid payload: {}", e);
            return;
        }
    };

    if let Err(e) = mailer
        .send_account_loggedin_email(&event.email, &event.firstname, &event.ip)
        .await
    {
        println!("[worker/logged_in_notification] Email error: {}", e);
    }

    println!(
        "[worker/account_loggedin_notification] Done: {}",
        event.email
    );
}

pub async fn handle_change_email(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: ChangeEmailEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/change_email] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/change_email] Invalid account_id");
            return;
        }
    };

    let (update, history): (
        Result<sqlx::postgres::PgQueryResult, sqlx::Error>,
        Result<sqlx::postgres::PgQueryResult, sqlx::Error>,
    ) = tokio::join!(
        sqlx::query!(
            "UPDATE accounts SET email = $1, updated_at = NOW() WHERE id = $2",
            event.new_email,
            account_uuid,
        )
        .execute(db),
        sqlx::query!(
            r#"INSERT INTO account_contact_history
       (account_id, field, old_value, new_value, changed_at)
       VALUES ($1, 'email', $2, $3, NOW())
       ON CONFLICT (account_id, field, old_value, new_value) DO NOTHING"#,
            account_uuid,
            event.old_email,
            event.new_email,
        )
        .execute(db)
    );

    if let Err(e) = update {
        println!("[worker/change_email] DB update error: {}", e);
        return;
    }

    if let Err(e) = history {
        println!("[worker/change_email] History insert error: {}", e);
    }

    if let Err(e) = mailer
        .send_email_changed(&event.new_email, &event.firstname, &event.old_email)
        .await
    {
        println!("[worker/change_email] Email error: {}", e);
    }

    println!(
        "[worker/change_email] Done: {} -> {}",
        event.old_email, event.new_email
    );
}

pub async fn handle_kyc_upgrade_status(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: KycUpgradeStatusEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            log::warn!("[worker/kyc_upgrade_status] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            log::warn!(
                "[worker/kyc_upgrade_status] Invalid account_id: {}",
                event.account_id
            );
            return;
        }
    };

    // ── Fetch account for email + firstname ───────────────────────────────────
    let row = match sqlx::query!(
        "SELECT email, firstname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            log::warn!(
                "[worker/kyc_upgrade_status] Account not found: {}",
                event.account_id
            );
            return;
        }
        Err(e) => {
            log::error!("[worker/kyc_upgrade_status] DB error: {}", e);
            return;
        }
    };

    let firstname = row.firstname.clone().unwrap_or_default();

    if event.status == "SUCCESS" {
        // ── Read pending_tier_upgrade into current_tier BEFORE nulling it ─────
        // Postgres evaluates all RHS values before applying writes,
        // so current_tier = pending_tier_upgrade safely captures the old value.
        let result = sqlx::query!(
            r#"UPDATE accounts SET
                current_tier              = pending_tier_upgrade,
                pending_tier_upgrade      = NULL,
                tier_upgrade_requested_at = NULL,
                tier_upgraded_at          = NOW(),
                account_number            = $1,
                account_name              = $2,
                updated_at                = NOW()
            WHERE id = $3"#,
            event.account_number,
            event.account_name,
            account_uuid
        )
        .execute(db)
        .await;

        match result {
            Ok(_) => {
                log::info!(
                    "[worker/kyc_upgrade_status] Tier {} upgraded: {}",
                    event.tier,
                    event.account_id
                );
                if let Err(e) = mailer
                    .send_tier_upgrade_approved(&row.email, &firstname, event.tier as u8)
                    .await
                {
                    log::error!("[worker/kyc_upgrade_status] Email error: {}", e);
                }
            }
            Err(e) => log::error!("[worker/kyc_upgrade_status] DB error: {}", e),
        }
    } else {
        // ── Rejected — clear both pending fields ──────────────────────────────
        let result = sqlx::query!(
            r#"UPDATE accounts SET
                pending_tier_upgrade      = NULL,
                tier_upgrade_requested_at = NULL,
                updated_at                = NOW()
            WHERE id = $1"#,
            account_uuid
        )
        .execute(db)
        .await;

        match result {
            Ok(_) => {
                log::info!(
                    "[worker/kyc_upgrade_status] Tier {} upgrade rejected: {} — {:?}",
                    event.tier,
                    event.account_id,
                    event.message
                );
                if let Err(e) = mailer
                    .send_tier_upgrade_rejected(
                        &row.email,
                        &firstname,
                        event.tier as u8,
                        event.message.as_deref(),
                    )
                    .await
                {
                    log::error!("[worker/kyc_upgrade_status] Email error: {}", e);
                }
            }
            Err(e) => log::error!("[worker/kyc_upgrade_status] DB error: {}", e),
        }
    }
}

pub async fn handle_open_wallet(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    tracing::info!("[open_wallet] ── Handler started ──────────────────────────");
    tracing::info!("[open_wallet] Raw payload: {}", payload);

    let event: OpenWalletEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/tier1_upgrade] Invalid payload: {}", e);
            return;
        }
    };

    // ── Parse UUID ────────────────────────────────────────────────────────────
    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => {
            tracing::info!("[open_wallet] Parsed UUID: {}", id);
            id
        }
        Err(e) => {
            tracing::error!(
                "[open_wallet] Invalid account_id '{}': {}",
                event.account_id,
                e
            );
            return;
        }
    };

    // ── Fetch account ─────────────────────────────────────────────────────────
    tracing::info!("[open_wallet] Fetching account from DB: {}", account_uuid);

    let row = match sqlx::query!(
        "SELECT current_tier, status, email FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => {
            tracing::info!(
                "[open_wallet] Account found — email: {}, status: {}, current_tier: {}",
                r.email,
                r.status,
                r.current_tier
            );
            r
        }
        Ok(None) => {
            tracing::warn!("[open_wallet] Account not found: {}", account_uuid);
            return;
        }
        Err(e) => {
            tracing::error!(
                "[open_wallet] DB error fetching account {}: {}",
                account_uuid,
                e
            );
            return;
        }
    };

    // ── Guards ────────────────────────────────────────────────────────────────
    if row.status != "active" {
        tracing::warn!(
            "[open_wallet] Account not active — status: '{}', account: {}",
            row.status,
            account_uuid
        );
        return;
    }

    if row.current_tier >= 1 {
        tracing::warn!(
            "[open_wallet] Already tier {} — skipping: {}",
            row.current_tier,
            account_uuid
        );
        return;
    }

    tracing::info!("[open_wallet] Guards passed — calling 9PSB");

    // ── Build request body ────────────────────────────────────────────────────
    let gender_code = match event.gender {
        Gender::MALE => 0,
        Gender::FEMALE => 1,
    };
    let other_names = format!("{} {}", event.firstname, event.othername);

    let open_wallet_body = serde_json::json!({
        "transactionTrackingRef": event.account_id,
        "lastName":               event.lastname,
        "otherNames":             other_names,
        "phoneNo":                event.phone_no,
        "gender":                 gender_code,
        "dateOfBirth":            event.date_of_birth,
        "address":                event.address,
        "nationalIdentityNo":     event.nin,
        "ninUserId":              event.nin_userid,
        "bvn":                    event.bvn,
        "email":                  row.email,
    });

    tracing::info!(
        "[open_wallet] 9PSB request body: {}",
        serde_json::to_string_pretty(&open_wallet_body)
            .unwrap_or_else(|_| open_wallet_body.to_string())
    );

    // ── Call 9PSB ─────────────────────────────────────────────────────────────
    tracing::info!("[open_wallet] Sending to 9PSB /waas/api/v1/open_wallet ...");

    let json = match PsbClient::new(cfg)
        .post(redis, "/waas/api/v1/open_wallet", &open_wallet_body)
        .await
    {
        Ok(j) => {
            tracing::info!(
                "[open_wallet] 9PSB response: {}",
                serde_json::to_string_pretty(&j).unwrap_or_else(|_| j.to_string())
            );
            j
        }
        Err(e) => {
            tracing::error!(
                "[open_wallet] 9PSB request failed for {}: {}",
                account_uuid,
                e
            );
            return;
        }
    };

    // ── Check status ──────────────────────────────────────────────────────────
    let status_str = json["status"].as_str().unwrap_or("").to_uppercase();
    tracing::info!("[open_wallet] 9PSB status field: '{}'", status_str);

    if status_str != "SUCCESS" {
        tracing::warn!(
            "[open_wallet] 9PSB rejected — status: '{}', account: {}, full response: {}",
            status_str,
            account_uuid,
            serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
        );
        return;
    }

    // ── Extract fields ────────────────────────────────────────────────────────
    let account_number = json["data"]["accountNumber"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let account_name = json["data"]["fullName"].as_str().unwrap_or("").to_string();

    tracing::info!(
        "[open_wallet] Extracted — account_number: '{}', account_name: '{}'",
        account_number,
        account_name
    );

    if account_number.is_empty() {
        tracing::error!(
            "[open_wallet] accountNumber empty in 9PSB response — account: {}, data: {:?}",
            account_uuid,
            json["data"]
        );
        return;
    }

    // ── Single DB write ───────────────────────────────────────────────────────
    tracing::info!(
        "[open_wallet] Writing to DB — account: {}, account_number: {}, account_name: {}",
        account_uuid,
        account_number,
        account_name
    );

    match sqlx::query!(
        r#"UPDATE accounts SET
            account_number   = $2,
            account_name     = $3,
            firstname        = $4,
            lastname         = $5,
            othername        = $6,
            bank_name        = $7,
            bvn              = $8,
            nin              = $9,
            nin_userid       = $10,
            phone_number     = $11,
            address          = $12,
            date_of_birth    = $13,
            gender           = $14,
            current_tier=1,
            tier_upgraded_at=  NOW(),
            updated_at       = NOW()
        WHERE id = $1"#,
        account_uuid,
        account_number,
        account_name,
        event.firstname,
        event.lastname,
        event.othername,
        "9PSB",
        event.bvn,
        event.nin,
        event.nin_userid,
        event.phone_no,
        event.address,
        event.date_of_birth,
        gender_code as i32,
    )
    .execute(db)
    .await
    {
        Ok(res) => {
            tracing::info!(
                "[open_wallet] DB write OK — rows_affected: {}, account: {}, account_number: {}",
                res.rows_affected(),
                account_uuid,
                account_number
            );
            if res.rows_affected() == 0 {
                tracing::warn!(
                    "[open_wallet] 0 rows affected — account may be deleted: {}",
                    account_uuid
                );
            }
        }
        Err(e) => {
            tracing::error!(
                "[open_wallet] DB write FAILED — account: {}, error: {}",
                account_uuid,
                e
            );
        }
    }

    tracing::info!(
        "[open_wallet] ── Complete — account: {}, account_number: {} ──",
        account_uuid,
        account_number
    );
}

pub async fn handle_tier2_upgrade(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: Tier2UpgradeEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/tier2_upgrade] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/tier2_upgrade] Invalid account_id");
            return;
        }
    };

    // ── Fetch account ─────────────────────────────────────────────────────────
    let account = sqlx::query!(
        "SELECT current_tier, status, pending_tier_upgrade, email, account_number, account_name FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await;

    let row = match account {
        Ok(Some(r)) => r,
        Ok(None) => {
            println!(
                "[worker/tier2_upgrade] Account not found: {}",
                event.account_id
            );
            return;
        }
        Err(e) => {
            println!("[worker/tier2_upgrade] DB error: {}", e);
            return;
        }
    };

    if row.status != "active" {
        println!(
            "[worker/tier2_upgrade] Account not active: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier < 1 {
        println!(
            "[worker/tier2_upgrade] Must be tier 1 before upgrading to tier 2: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier >= 2 {
        println!(
            "[worker/tier2_upgrade] Already tier 2+: {}",
            event.account_id
        );
        return;
    }

    if row.pending_tier_upgrade.is_some() {
        println!(
            "[worker/tier2_upgrade] Upgrade already in progress: {}",
            event.account_id
        );
        return;
    }

    // ── Must have account number from tier 1 ──────────────────────────────────
    let account_number = match row.account_number.clone() {
        Some(n) => n,
        None => {
            println!(
                "[worker/tier2_upgrade] No account number — must complete tier 1 first: {}",
                event.account_id
            );
            return;
        }
    };

    let account_name = row.account_name.clone().unwrap_or_default();

    // ── Set pending ───────────────────────────────────────────────────────────
    if let Err(e) = sqlx::query!(
        r#"UPDATE accounts SET
            pending_tier_upgrade = 2,
            tier_upgrade_requested_at = NOW(),
            updated_at = NOW()
        WHERE id = $1"#,
        account_uuid
    )
    .execute(db)
    .await
    {
        println!("[worker/tier2_upgrade] Failed to set pending: {}", e);
        return;
    }

    // ── Call 9PSB wallet_upgrade ──────────────────────────────────────────────
    let psb = PsbClient::new(cfg);

    let upgrade_body = serde_json::json!({
        "accountNumber": account_number,
        "bvn": event.bvn,
        "nin": event.nin,
        "accountName": account_name,
        "phoneNumber": event.phone_no,
        "tier": "2",
        "email": row.email,
        "userPhoto": event.user_photo,
        "idType": event.id_type.to_string(),
        "idNumber": event.id_number,
        "idIssueDate": event.id_issue_date,
        "idExpiryDate": event.id_expiry_date,
        "idCardFront": event.id_card_front,
        "idCardBack": event.id_card_back,
        "houseNumber": event.house_number,
        "streetName": event.street_name,
        "state": event.state,
        "city": event.city,
        "localGovernment": event.local_government,
        "pep": event.pep,
        "customerSignature": event.customer_signature,
        "utilityBill": event.utility_bill,
        "nearestLandmark": event.nearest_landmark,
        "placeOfBirth": event.place_of_birth,
        "proofOfAddressVerification": event.proof_of_address,
    });

    match psb
        .post(redis, "/waas/api/v1/wallet_upgrade", &upgrade_body)
        .await
    {
        Ok(json) => {
            println!("[worker/tier2_upgrade] wallet_upgrade response: {:?}", json);

            let is_success =
                json["status"].as_str().map(|s| s.to_uppercase()) == Some("SUCCESS".to_string());

            if is_success {
                println!(
                    "[worker/tier2_upgrade] Submitted successfully, awaiting webhook: {}",
                    row.email
                );
            } else {
                println!("[worker/tier2_upgrade] wallet_upgrade rejected: {:?}", json);
                // clear pending so user can retry
                let _ = sqlx::query!(
                    "UPDATE accounts SET pending_tier_upgrade = NULL, updated_at = NOW() WHERE id = $1",
                    account_uuid
                )
                .execute(db)
                .await;
            }
        }
        Err(e) => {
            println!("[worker/tier2_upgrade] wallet_upgrade error: {}", e);
            // keep pending — allow retry via Kafka
        }
    }
}

pub async fn handle_tier3_upgrade(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: Tier3UpgradeEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/tier3_upgrade] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/tier3_upgrade] Invalid account_id");
            return;
        }
    };

    // ── Fetch account ─────────────────────────────────────────────────────────
    let row = match sqlx::query!(
        "SELECT current_tier, status, pending_tier_upgrade, email,
                account_number, account_name, bvn, nin
         FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            println!(
                "[worker/tier3_upgrade] Account not found: {}",
                event.account_id
            );
            return;
        }
        Err(e) => {
            println!("[worker/tier3_upgrade] DB error: {}", e);
            return;
        }
    };

    // ── Guards ────────────────────────────────────────────────────────────────
    if row.status != "active" {
        println!(
            "[worker/tier3_upgrade] Account not active: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier < 2 {
        println!(
            "[worker/tier3_upgrade] Must be tier 2 before upgrading to tier 3: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier >= 3 {
        println!(
            "[worker/tier3_upgrade] Already tier 3: {}",
            event.account_id
        );
        return;
    }

    if row.pending_tier_upgrade.is_some() {
        println!(
            "[worker/tier3_upgrade] Upgrade already in progress: {}",
            event.account_id
        );
        return;
    }

    let account_number = match row.account_number.clone() {
        Some(n) => n,
        None => {
            println!(
                "[worker/tier3_upgrade] No account number: {}",
                event.account_id
            );
            return;
        }
    };

    // ── Resolve BVN and NIN ───────────────────────────────────────────────────
    let bvn = event.bvn.clone().or(row.bvn.clone()).unwrap_or_default();
    let nin = event.nin.clone().or(row.nin.clone()).unwrap_or_default();

    if bvn.is_empty() || nin.is_empty() {
        println!(
            "[worker/tier3_upgrade] Missing BVN or NIN: {}",
            event.account_id
        );
        return;
    }

    // ── Set pending ───────────────────────────────────────────────────────────
    if let Err(e) = sqlx::query!(
        r#"UPDATE accounts SET
            pending_tier_upgrade = 3,
            tier_upgrade_requested_at = NOW(),
            updated_at = NOW()
        WHERE id = $1"#,
        account_uuid
    )
    .execute(db)
    .await
    {
        println!("[worker/tier3_upgrade] Failed to set pending: {}", e);
        return;
    }

    // ── Call 9PSB wallet_upgrade ──────────────────────────────────────────────
    let upgrade_body = serde_json::json!({
        "accountNumber": account_number,
        "bvn":           bvn,
        "nin":           nin,
        "proofOfAddressVerification": event.proof_of_address,
    });

    match PsbClient::new(cfg)
        .post(
            redis,
            "/waas/api/v1/walletUpgrade-tier3-base64",
            &upgrade_body,
        )
        .await
    {
        Ok(json) => {
            println!("[worker/tier3_upgrade] wallet_upgrade response: {:?}", json);

            let is_success =
                json["status"].as_str().map(|s| s.to_uppercase()) == Some("SUCCESS".to_string());

            if is_success {
                println!(
                    "[worker/tier3_upgrade] Submitted successfully, awaiting webhook: {}",
                    row.email
                );
            } else {
                println!("[worker/tier3_upgrade] wallet_upgrade rejected: {:?}", json);
                let _ = sqlx::query!(
                    "UPDATE accounts SET pending_tier_upgrade = NULL, updated_at = NOW() WHERE id = $1",
                    account_uuid
                )
                .execute(db)
                .await;
            }
        }
        Err(e) => {
            println!("[worker/tier3_upgrade] wallet_upgrade error: {}", e);
            let _ = sqlx::query!(
                "UPDATE accounts SET pending_tier_upgrade = NULL, updated_at = NOW() WHERE id = $1",
                account_uuid
            )
            .execute(db)
            .await;
        }
    }
}

pub async fn handle_legacy_beneficiary_added(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: LegacyPlanBeneficiaryAddedEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/legacy_beneficiary_added] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/legacy_beneficiary_added] Invalid account_id");
            return;
        }
    };

    // ── Fetch owner name once ─────────────────────────────────────────────────
    let owner = match sqlx::query!(
        "SELECT firstname, lastname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => r,
        _ => {
            println!(
                "[worker/legacy_beneficiary_added] Owner not found: {}",
                event.account_id
            );
            return;
        }
    };

    let owner_name = format!(
        "{} {}",
        owner.firstname.unwrap_or_default(),
        owner.lastname.unwrap_or_default()
    )
    .trim()
    .to_string();

    // ── Send email to each beneficiary ────────────────────────────────────────
    for kin in &event.next_of_kin {
        if let Err(e) = mailer
            .send_legacy_beneficiary_added(
                &kin.email,
                &kin.fullname,
                &owner_name,
                kin.share_percentage,
            )
            .await
        {
            println!(
                "[worker/legacy_beneficiary_added] Email failed for {}: {}",
                kin.email, e
            );
        }
    }

    println!(
        "[worker/legacy_beneficiary_added] Done — notified {} beneficiaries for account: {}",
        event.next_of_kin.len(),
        event.account_id
    );
}

pub async fn handle_legacy_beneficiary_deleted(payload: &str, db: &PgPool, mailer: &Mailer) {
    let event: LegacyPlanBeneficiaryDeletedEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/legacy_beneficiary_deleted] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            println!("[worker/legacy_beneficiary_deleted] Invalid account_id");
            return;
        }
    };

    // ── Fetch owner name once ─────────────────────────────────────────────────
    let owner = match sqlx::query!(
        "SELECT firstname, lastname FROM accounts WHERE id = $1",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => r,
        _ => {
            println!(
                "[worker/legacy_beneficiary_deleted] Owner not found: {}",
                event.account_id
            );
            return;
        }
    };

    let owner_name = format!(
        "{} {}",
        owner.firstname.unwrap_or_default(),
        owner.lastname.unwrap_or_default()
    )
    .trim()
    .to_string();

    // ── Send email to each beneficiary ────────────────────────────────────────
    for kin in &event.next_of_kin {
        if let Err(e) = mailer
            .send_legacy_beneficiary_deleted(&kin.email, &kin.fullname, &owner_name)
            .await
        {
            println!(
                "[worker/legacy_beneficiary_deleted] Email failed for {}: {}",
                kin.email, e
            );
        }
    }

    println!(
        "[worker/legacy_beneficiary_deleted] Done — notified {} beneficiaries for account: {}",
        event.next_of_kin.len(),
        event.account_id
    );
}

pub async fn handle_set_payment_pin(payload: &str, mailer: &Mailer) {
    let event: SetPaymentPinEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!(
                "[handle_set_payment_pin] Failed to deserialize payload: {}",
                e
            );
            return;
        }
    };

    println!(
        "[handle_set_payment_pin] Sending PIN set notification to {}",
        event.email
    );

    if let Err(e) = mailer
        .send_payment_pin_set(&event.email, &event.firstname)
        .await
    {
        println!("[handle_set_payment_pin] Mailer error: {}", e);
    }
}

/// Formats a numeric string into a comma-separated currency format.
/// e.g. "100000" -> "100,000" | "1000000.50" -> "1,000,000.50"
fn format_amount(amount: &str) -> String {
    let amount = amount.trim();

    let (integer_part, decimal_part) = if let Some(dot_pos) = amount.find('.') {
        (&amount[..dot_pos], Some(&amount[dot_pos + 1..]))
    } else {
        (amount, None)
    };

    let formatted_integer = integer_part
        .chars()
        .rev()
        .enumerate()
        .flat_map(|(i, c)| {
            if i > 0 && i % 3 == 0 {
                vec![',', c]
            } else {
                vec![c]
            }
        })
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();

    match decimal_part {
        Some(dec) => format!("{}.{}", formatted_integer, dec),
        None => formatted_integer,
    }
}

pub async fn handle_internal_transfer(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: InternalTransferInitiatedEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(_e) => {
            return;
        }
    };

    // ── Parse UUIDs once ──────────────────────────────────────────────────────
    let sender_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_e) => {
            return;
        }
    };

    let reciever_uuid = match uuid::Uuid::parse_str(&event.reciever_id) {
        Ok(id) => id,
        Err(_e) => {
            return;
        }
    };

    // ── Format amount once for reuse across notifications ─────────────────────
    let formatted_amount = format_amount(&event.amount);

    // ── 1. PSB debit sender ───────────────────────────────────────────────────
    let debit_payload = serde_json::json!({
        "accountNo":     event.sender_account,
        "narration":     event.narration,
        "totalAmount":   event.amount.parse::<f64>().unwrap_or(0.0),
        "transactionId": event.reference,
        "merchant": {
            "isFee":              false,
            "merchantFeeAccount": "",
            "merchantFeeAmount":  ""
        }
    });

    match PsbClient::new(cfg)
        .post(redis, "/waas/api/v1/debit/transfer", &debit_payload)
        .await
    {
        Ok(res) => {
            let status = res["status"].as_str().unwrap_or("").to_uppercase();
            if status != "SUCCESS" {
                let _ = insert_transaction(
                    db,
                    sender_uuid,
                    reciever_uuid,
                    &event.reference,
                    "debit",
                    &event.amount,
                    &event.narration,
                    "failed",
                )
                .await;
                return;
            }
        }
        Err(_e) => {
            return;
        }
    }

    // ── 2. PSB credit reciever ────────────────────────────────────────────────
    let credit_ref = format!("{}-CR", event.reference);

    let credit_payload = serde_json::json!({
        "accountNo":     event.recipient_account,
        "narration":     event.narration,
        "totalAmount":   event.amount.parse::<f64>().unwrap_or(0.0),
        "transactionId": credit_ref,
        "merchant": {
            "isFee":              false,
            "merchantFeeAccount": "",
            "merchantFeeAmount":  ""
        }
    });

    match PsbClient::new(cfg)
        .post(redis, "/waas/api/v1/credit/transfer", &credit_payload)
        .await
    {
        Ok(res) => {
            let status = res["status"].as_str().unwrap_or("").to_uppercase();
            if status != "SUCCESS" {
                // ── Debit succeeded but credit failed — record both ────────────
                let failed_credit_ref = format!("{}-CR", event.reference);
                let _ = tokio::join!(
                    insert_transaction(
                        db,
                        sender_uuid,
                        reciever_uuid,
                        &event.reference,
                        "debit",
                        &event.amount,
                        &event.narration,
                        "success",
                    ),
                    insert_transaction(
                        db,
                        sender_uuid,
                        reciever_uuid,
                        &failed_credit_ref,
                        "credit",
                        &event.amount,
                        &event.narration,
                        "failed",
                    )
                );
                // ── Notify sender only — credit failed so reciever gets nothing ─
                let sender_token = sqlx::query_scalar!(
                    "SELECT device_token FROM accounts WHERE id = $1",
                    sender_uuid
                )
                .fetch_optional(db)
                .await
                .ok()
                .flatten()
                .flatten();

                if let Some(token) = sender_token {
                    send_push_notification(
                        &token,
                        "Pressend Transfer Issue",
                        &format!(
                            "Your transfer of ₦{} could not be completed. You will be refunded.",
                            formatted_amount
                        ),
                        Some(serde_json::json!({
                            "route":     "NotificationDetail",
                            "service":   "transfer_sent",
                            "status":    "failed",
                            "amount":    formatted_amount,
                            "reference": event.reference,
                            "narration": "Transfer could not be completed. Refund in progress.",
                        })),
                    )
                    .await;
                }
                return;
            }
        }
        Err(_e) => {
            return;
        }
    }

    // ── 3. Both legs succeeded — insert DB records concurrently ──────────────
    let success_credit_ref = format!("{}-CR", event.reference);

    let (debit_result, credit_result) = tokio::join!(
        insert_transaction(
            db,
            sender_uuid,
            reciever_uuid,
            &event.reference,
            "debit",
            &event.amount,
            &event.narration,
            "success",
        ),
        insert_transaction(
            db,
            sender_uuid,
            reciever_uuid,
            &success_credit_ref,
            "credit",
            &event.amount,
            &event.narration,
            "success",
        )
    );

    if let Err(e) = debit_result {
        println!("[worker/internal_transfer] Debit DB insert error: {}", e);
    }
    if let Err(e) = credit_result {
        println!("[worker/internal_transfer] Credit DB insert error: {}", e);
    }

    // ── 4. Push notification — sender only ────────────────────────────────────
    // Receiver credit alert is handled by  via PSB webhook
    let sender_info = sqlx::query!(
        "SELECT device_token, firstname, lastname FROM accounts WHERE id = $1",
        sender_uuid
    )
    .fetch_optional(db)
    .await;

    let reciever_info = sqlx::query!(
        "SELECT firstname, lastname FROM accounts WHERE id = $1",
        reciever_uuid
    )
    .fetch_optional(db)
    .await;

    // ── Build recipient display name for sender notification ──────────────────
    let recipient_display_name = match &reciever_info {
        Ok(Some(row)) => {
            let name = format!(
                "{} {}",
                row.firstname.as_deref().unwrap_or(""),
                row.lastname.as_deref().unwrap_or("")
            )
            .trim()
            .to_string();
            if name.is_empty() {
                event.recipient_account.clone()
            } else {
                name
            }
        }
        _ => event.recipient_account.clone(),
    };

    // Notify Sender (Confirmation)
    if let Ok(Some(row)) = &sender_info {
        if let Some(token) = &row.device_token {
            send_push_notification(
                token,
                "Transfer Successful",
                &format!(
                    "₦{} sent successfully to {}",
                    formatted_amount, recipient_display_name
                ),
                Some(serde_json::json!({
                    "route":     "NotificationDetail",
                    "service":   "transfer_sent",
                    "status":    "success",
                    "amount":    formatted_amount,
                    "reference": event.reference,
                    "recipient": recipient_display_name,
                })),
            )
            .await;
        }
    }

    println!(
        "[worker/internal_transfer] ── Complete — ref: {}, sender: {}, reciever: {}, amount: {} ──",
        event.reference, event.sender_account, event.recipient_account, event.amount
    );
}


pub async fn handle_transfer_inflow(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: InboundTransferEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/transfer_inflow] Invalid payload: {}", e);
            return;
        }
    };

    let formatted_amount = format_amount(&event.amount);
    let amount_clean = event.amount.replace(",", "");
    let amount_bd = bigdecimal::BigDecimal::from_str(&amount_clean).unwrap_or_default();
    let amount_f64: f64 = amount_clean.parse().unwrap_or(0.0);
    let is_above_10k = amount_f64 > 10_000.0;

    // ── Check if transactionref already exists in our system ─────────────────
    let existing = sqlx::query!(
        "SELECT id, status FROM transactions WHERE reference = $1",
        event.transaction_ref
    )
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    if let Some(tx) = existing {
        // ── Known transaction — update status to whatever webhook says ────────
        let status_str = event.status.as_deref().unwrap_or("").to_lowercase();

        let new_status = if status_str == "failed" {
                "failed"
            } else {
                "success"
            };
        let update = sqlx::query!(
            r#"
            UPDATE transactions
            SET
                status     = $1,
                updated_at = NOW(),
                meta       = meta || $2::jsonb
            WHERE id = $3
            "#,
            new_status,
            serde_json::json!({
                "psb_transaction_ref": event.transaction_ref,
                "nip_session_id":      event.session_id,
                "sender_name":         event.sender_name,
                "sender_account":      event.sender_account,
                "sender_bank":         event.sender_bank,
            }),
            tx.id,
        )
        .execute(db)
        .await;

        match update {
            Ok(_) => println!(
                "[worker/transfer_inflow] Updated transaction — ref: {}, status: {}",
                event.transaction_ref, new_status
            ),
            Err(e) => println!(
                "[worker/transfer_inflow] DB update error — ref: {}, err: {}",
                event.transaction_ref, e
            ),
        }

        return;
    }

    // ── transactionref not found — external inbound transfer ──────────────────
    // Fetch receiver by account number
    let receiver = match sqlx::query!(
        r#"
        SELECT id, device_token, account_type, account_name
        FROM accounts
        WHERE account_number = $1 AND deleted_at IS NULL
        "#,
        event.account_number
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            println!(
                "[worker/transfer_inflow] Account not found: {}",
                event.account_number
            );
            return;
        }
        Err(e) => {
            println!("[worker/transfer_inflow] DB error: {}", e);
            return;
        }
    };

    let is_business = receiver.account_type == "business";
    let should_charge_fee = is_business && is_above_10k;

    // ── Insert external inflow as credit transaction ───────────────────────────
    let result = sqlx::query!(
        r#"
        INSERT INTO transactions (
            sender_id,
            reciever_id,
            reference,
            type,
            amount,
            currency,
            narration,
            status,
            channel,
            reciever_account_number,
            reciever_account_name,
            reciever_bank,
            meta
        )
        VALUES (
            NULL, $1, $2, 'credit', $3, 'NGN', $4, 'success', 'external',
            $5, $6, '9PSB', $7
        )
        ON CONFLICT (reference) DO NOTHING
        "#,
        receiver.id,
        event.transaction_ref,
        amount_bd,
        event.narration,
        event.account_number,
        receiver.account_name.clone().unwrap_or_default(),
        serde_json::json!({
            "sender_name":     event.sender_name,
            "sender_account":  event.sender_account,
            "sender_bank":     event.sender_bank,
            "transaction_ref": event.transaction_ref,
            "nip_session_id":  event.session_id,
        }),
    )
    .execute(db)
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => {
            println!(
                "[worker/transfer_inflow] Duplicate — already inserted — ref: {}",
                event.transaction_ref
            );
            return;
        }
        Ok(_) => println!(
            "[worker/transfer_inflow] External inflow recorded — ref: {}, account: {}, amount: ₦{}",
            event.transaction_ref, event.account_number, formatted_amount
        ),
        Err(e) => {
            println!("[worker/transfer_inflow] DB insert error: {}", e);
            return;
        }
    }

    // ── Debit ₦35 fee if business account and amount > ₦10,000 ───────────────
    if should_charge_fee {
        let fee_ref = format!("{}-FEE", event.transaction_ref);

        let fee_payload = serde_json::json!({
            "accountNo":     event.account_number,
            "narration":     "Incoming transfer processing fee",
            "totalAmount":   "0",
            "transactionId": fee_ref,
            "merchant": {
                "isFee":              true,
                "merchantFeeAccount": cfg._9psb_operational_account,
                "merchantFeeAmount":  "35"
            }
        });

        match PsbClient::new(cfg)
            .post(redis, "/waas/api/v1/debit/transfer", &fee_payload)
            .await
        {
            Ok(res) => {
                let status = res["status"].as_str().unwrap_or("").to_uppercase();
                if status == "SUCCESS" {
                    println!(
                        "[worker/transfer_inflow] Fee deducted — ref: {}",
                        fee_ref
                    );

                    let fee_bd = bigdecimal::BigDecimal::from_str("35").unwrap_or_default();
                    let _ = sqlx::query!(
                        r#"
                        INSERT INTO transactions (
                            sender_id, reciever_id, reference, type, amount, currency,
                            narration, status, channel, meta
                        )
                        VALUES ($1, NULL, $2, 'debit', $3, 'NGN',
                                'Incoming transfer processing fee', 'success', 'internal', $4)
                        ON CONFLICT (reference) DO NOTHING
                        "#,
                        receiver.id,
                        fee_ref,
                        fee_bd,
                        serde_json::json!({
                            "fee_type":   "incoming_transfer_fee",
                            "linked_ref": event.transaction_ref,
                            "fee_amount": "35.00",
                        }),
                    )
                    .execute(db)
                    .await
                    .map_err(|e| println!("[worker/transfer_inflow] Fee DB insert error: {}", e));

                    // ── Notify business owner about fee ────────────────────────
                    if let Some(token) = &receiver.device_token {
                        send_push_notification(
                            token,
                            "Processing Fee Deducted",
                            &format!(
                                "A processing fee of ₦35.00 has been deducted from your \
                                 account for an incoming transfer of ₦{} from {} ({}).",
                                formatted_amount, event.sender_name, event.sender_bank
                            ),
                            Some(serde_json::json!({
                                "route":     "NotificationDetail",
                                "service":   "transfer_fee",
                                "status":    "success",
                                "amount":    "35.00",
                                "reference": fee_ref,
                                "narration": format!(
                                    "Processing fee for ₦{} inflow from {} · {}",
                                    formatted_amount, event.sender_name, event.sender_bank
                                ),
                            })),
                        )
                        .await;
                    }
                } else {
                    println!(
                        "[worker/transfer_inflow] Fee rejected by PSB — status: '{}', ref: {}",
                        status, fee_ref
                    );
                }
            }
            Err(e) => println!(
                "[worker/transfer_inflow] Fee PSB error — ref: {}, err: {}",
                fee_ref, e
            ),
        }
    }

    // ── Credit alert push notification ────────────────────────────────────────
    if let Some(token) = &receiver.device_token {
        let (title, body) = if should_charge_fee {
            (
                "Credit Alert",
                format!(
                    "Your wallet has been credited with ₦{}. \
                     A processing fee of ₦35.00 was applied as this transfer exceeds ₦10,000. \
                     Sent by: {} ({}).",
                    formatted_amount, event.sender_name, event.sender_bank
                ),
            )
        } else {
            (
                "Credit Alert",
                format!(
                    "Your wallet has been credited with ₦{}. \
                     Sent by: {} ({}).",
                    formatted_amount, event.sender_name, event.sender_bank
                ),
            )
        };

        send_push_notification(
            token,
            title,
            &body,
            Some(serde_json::json!({
                "route":     "NotificationDetail",
                "service":   "transfer_received",
                "status":    "success",
                "amount":    formatted_amount,
                "reference": event.transaction_ref,
                "sender":    event.sender_name,
                "narration": format!("From {} · {}", event.sender_name, event.sender_bank),
            })),
        )
        .await;
    }

    println!(
        "[worker/transfer_inflow] ── Complete — ref: {}, account: {}, amount: ₦{}, fee_charged: {} ──",
        event.transaction_ref, event.account_number, formatted_amount, should_charge_fee
    );
}



// ── Sharedp insert helper ──────────────────────────────────────────────────────

async fn insert_transaction(
    db: &PgPool,
    sender_uuid: uuid::Uuid,
    reciever_uuid: uuid::Uuid,
    reference: &str,
    tx_type: &str,
    amount: &str,
    narration: &str,
    status: &str,
) -> Result<(), sqlx::Error> {
    let amount_bd = bigdecimal::BigDecimal::from_str(amount).unwrap_or_default();

    sqlx::query!(
        r#"
        INSERT INTO transactions
            (sender_id, reciever_id, reference, type, amount, currency, narration, status, channel)
        VALUES
            ($1, $2, $3, $4, $5, 'NGN', $6, $7, 'internal')
        ON CONFLICT (reference) DO NOTHING
        "#,
        sender_uuid,
        reciever_uuid,
        reference,
        tx_type,
        amount_bd,
        narration,
        status,
    )
    .execute(db)
    .await?;

    Ok(())
}

pub async fn handle_external_transfer(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: ExternalTransferInitiatedEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/external_transfer] Invalid payload: {}", e);
            return;
        }
    };

    println!(
        "[worker/external_transfer] Processing — sender: {}, recipient: {}, bank: {}, amount: {}, ref: {}",
        event.sender_account_number,
        event.recipient_number,
        event.bank_code,
        event.amount,
        event.reference
    );

    let sender_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(e) => {
            println!("[worker/external_transfer] Invalid sender uuid: {}", e);
            return;
        }
    };

    // ── Format amount once for reuse across notifications ─────────────────────
    let formatted_amount = format_amount(&event.amount);

    // ── Call 9PSB wallet_other_banks ──────────────────────────────────────────
    let psb_payload = serde_json::json!({
        "customer": {
            "account": {
                "bank":                event.bank_code,
                "name":                event.recipient_name,
                "number":              event.recipient_number,
                "senderaccountnumber": event.sender_account_number,
                "sendername":          event.sender_name,
            }
        },
        "narration": event.narration,
        "order": {
            "amount":      event.amount,
            "country":     "NGA",
            "currency":    "NGN",
            "description": event.narration,
        },
        "transaction": {
            "reference": event.reference,
        },
        "merchant": {
            "isFee":              false,
            "merchantFeeAccount": "",
            "merchantFeeAmount":  "" 
        }
    });

    let amount_bd = bigdecimal::BigDecimal::from_str(&event.amount).unwrap_or_default();

    match PsbClient::new(cfg)
        .post(redis, "/waas/api/v1/wallet_other_banks", &psb_payload)
        .await
    {
        Ok(res) => {
            println!("[worker/external_transfer] PSB response: {:?}", res);

            let status = res["status"].as_str().unwrap_or("").to_uppercase();

            let (tx_status, push_title, push_body) = if status == "SUCCESS" {
                (
                    "success",
                    "Transfer Successful",
                    format!(
                        "Dear valued customer, your transfer of ₦{} to {} has been \
                         completed successfully. Reference: {}.",
                        formatted_amount, event.recipient_name, event.reference
                    ),
                )
            } else {
                (
                    "failed",
                    "Transfer Unsuccessful",
                    format!(
                        "Dear valued customer, we were unable to process your transfer \
                         of ₦{} to {}. Please try again or contact support if the issue \
                         persists. Reference: {}.",
                        formatted_amount, event.recipient_name, event.reference
                    ),
                )
            };

            // ── Record transaction ────────────────────────────────────────────
            let _ = sqlx::query!(
                r#"
                INSERT INTO transactions
                    (sender_id, reciever_id, reference, type, amount, currency,
                     narration, status, channel,
                     reciever_account_number, reciever_account_name, reciever_bank,
                     meta)
                VALUES
                    ($1, NULL, $2, 'debit', $3, 'NGN', $4, $5, 'external',
                     $6, $7, $8, $9)
                ON CONFLICT (reference) DO NOTHING
                "#,
                sender_uuid,
                event.reference,
                amount_bd,
                event.narration,
                tx_status,
                event.recipient_number,
                event.recipient_name,
                event.bank_code,
                serde_json::json!({
                    "recipient_name":   event.recipient_name,
                    "recipient_number": event.recipient_number,
                    "bank_code":        event.bank_code,
                    "psb_response_code": res["responseCode"].as_str().unwrap_or(""),
                }),
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/external_transfer] DB insert error: {}", e));

            // ── Push notification ─────────────────────────────────────────────
            let token = sqlx::query_scalar!(
                "SELECT device_token FROM accounts WHERE id = $1",
                sender_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .flatten();

            if let Some(token) = token {
                send_push_notification(
        &token,
        push_title,
        &push_body,
        Some(serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "external_transfer",
            "status":    if status == "SUCCESS" { "success" } else { "failed" },
            "amount":    formatted_amount,
            "reference": event.reference,
            "recipient": event.recipient_name,
            "narration": format!("{} · {}", event.recipient_name, event.recipient_number),
        })),
    )
    .await;
            }

            println!(
                "[worker/external_transfer] ── Complete — ref: {}, status: {} ──",
                event.reference, tx_status
            );
        }
        Err(e) => {
            println!(
                "[worker/external_transfer] PSB error — ref: {}, error: {}",
                event.reference, e
            );
        }
    }
}


pub async fn handle_airtime_purchase(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: AirtimePurchaseEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/airtime_purchase] Invalid payload: {}", e);
            return;
        }
    };

    println!(
        "[worker/airtime_purchase] Processing — account: {}, phone: {}, network: {}, amount: {}, ref: {}",
        event.account_number, event.phone_number, event.network, event.amount, event.reference
    );

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(e) => {
            println!("[worker/airtime_purchase] Invalid account_id: {}", e);
            return;
        }
    };

    let amount_bd = bigdecimal::BigDecimal::from_str(&event.amount).unwrap_or_default();

    // ── Call VAS ──────────────────────────────────────────────────────────────
    let vas_payload = serde_json::json!({
        "phoneNumber":          event.phone_number,
        "network":              event.network,
        "amount":               event.amount,
        "debitAccount":         event.account_number,
        "transactionReference": event.reference,
    });

    match VasClient::new(cfg)
        .post(redis, "/vas/api/v1/topup/airtime", &vas_payload)
        .await
    {
        Ok(res) => {
            let vas_status = res["status"].as_str().unwrap_or("").to_uppercase();
            let tx_status = if vas_status == "SUCCESS" {
                "success"
            } else {
                "failed"
            };

            // ── Save to vas_transactions ──────────────────────────────────────
            let _ = sqlx::query!(
                r#"
                INSERT INTO vas_transactions
                    (account_id, vas_type, network, recipient, amount, currency,
                     reference, debit_account, status, response_code, response_message, meta)
                VALUES
                    ($1, 'airtime', $2, $3, $4, 'NGN', $5, $6, $7, $8, $9, $10)
                ON CONFLICT (reference) DO NOTHING
                "#,
                account_uuid,
                event.network,
                event.phone_number,
                amount_bd,
                event.reference,
                event.account_number,
                tx_status,
                res["responseCode"].as_str().unwrap_or(""),
                res["message"].as_str().unwrap_or(""),
                serde_json::json!({
                    "phone_number": event.phone_number,
                    "network":      event.network,
                }),
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/airtime_purchase] DB insert error: {}", e));

            // ── Push notification ─────────────────────────────────────────────
            let token = sqlx::query_scalar!(
                "SELECT device_token FROM accounts WHERE id = $1",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .flatten();

            if let Some(token) = token {
                let (title, body) = if vas_status == "SUCCESS" {
                    (
                        "Airtime Purchase Successful",
                        format!(
                            "Dear valued customer, your airtime recharge of ₦{} to {} ({}) has been completed successfully.",
                            event.amount, event.phone_number, event.network
                        ),
                    )
                } else {
                    (
                        "Airtime Purchase Unsuccessful",
                        format!(
                            "Dear valued customer, we were unable to process your airtime recharge of ₦{} to {}. Please try again or contact support. Reference: {}.",
                            event.amount, event.phone_number, event.reference
                        ),
                    )
                };

                send_push_notification(
                    &token,
                    title,
                    &body,
                    Some(serde_json::json!({
                        "route":     "NotificationDetail",
                        "service":   "airtime_purchase",
                        "status":    tx_status,
                        "amount":    event.amount,
                        "reference": event.reference,
                        "recipient": event.phone_number,
                        "network":   event.network,
                    })),
                )
                .await;
            }

            println!(
                "[worker/airtime_purchase] ── Complete — ref: {}, status: {} ──",
                event.reference, tx_status
            );
        }
        Err(e) => {
            println!(
                "[worker/airtime_purchase] VAS error — ref: {}, error: {}",
                event.reference, e
            );

            // ── Save failed attempt ───────────────────────────────────────────
            let _ = sqlx::query!(
                r#"
                INSERT INTO vas_transactions
                    (account_id, vas_type, network, recipient, amount, currency,
                     reference, debit_account, status, response_message)
                VALUES
                    ($1, 'airtime', $2, $3, $4, 'NGN', $5, $6, 'failed', $7)
                ON CONFLICT (reference) DO NOTHING
                "#,
                account_uuid,
                event.network,
                event.phone_number,
                amount_bd,
                event.reference,
                event.account_number,
                e.to_string(),
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/airtime_purchase] DB failed insert error: {}", e));

            // ── Push notification — failed ─────────────────────────────────────
            let token = sqlx::query_scalar!(
                "SELECT device_token FROM accounts WHERE id = $1",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .flatten();

            if let Some(token) = token {
                send_push_notification(
        &token,
        "Airtime Purchase Unsuccessful",
        &format!(
            "Dear valued customer, we were unable to process your airtime recharge of ₦{} to {}. Please try again or contact support. Reference: {}.",
            event.amount, event.phone_number, event.reference
        ),
        Some(serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "airtime_purchase",
            "status":    "failed",
            "amount":    event.amount,
            "reference": event.reference,
            "recipient": event.phone_number,
            "network":   event.network,
        })),
    )
    .await;
            }
        }
    }
}

pub async fn handle_data_purchase(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: DataPurchaseEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            println!("[worker/data_purchase] Invalid payload: {}", e);
            return;
        }
    };


    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(e) => {
            println!("[worker/data_purchase] Invalid account_id: {}", e);
            return;
        }
    };

    let amount_bd = bigdecimal::BigDecimal::from_str(&event.amount).unwrap_or_default();

    // ── Call VAS ──────────────────────────────────────────────────────────────
    let vas_payload = serde_json::json!({
        "phoneNumber":          event.phone_number,
        "network":              event.network,
        "productId":            event.product_id,
        "amount":               event.amount,
        "debitAccount":         event.account_number,
        "transactionReference": event.reference,
    });

    let formatted_amount = format_amount(&event.amount);


    match VasClient::new(cfg)
        .post(redis, "/vas/api/v1/topup/data", &vas_payload)
        .await
    {
        Ok(res) => {
            let vas_status = res["status"].as_str().unwrap_or("").to_uppercase();
            let tx_status = if vas_status == "SUCCESS" {
                "success"
            } else {
                "failed"
            };

            // ── Save to vas_transactions ──────────────────────────────────────
            let _ = sqlx::query!(
                r#"
                INSERT INTO vas_transactions
                    (account_id, vas_type, network, recipient, amount, currency,
                     reference, debit_account, status, response_code, response_message, meta)
                VALUES
                    ($1, 'data', $2, $3, $4, 'NGN', $5, $6, $7, $8, $9, $10)
                ON CONFLICT (reference) DO NOTHING
                "#,
                account_uuid,
                event.network,
                event.phone_number,
                amount_bd,
                event.reference,
                event.account_number,
                tx_status,
                res["responseCode"].as_str().unwrap_or(""),
                res["message"].as_str().unwrap_or(""),
                serde_json::json!({
                    "phone_number": event.phone_number,
                    "network":      event.network,
                    "product_id":   event.product_id,
                    "data_plan":    res["data"]["dataPlan"].as_str().unwrap_or(""),
                }),
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/data_purchase] DB insert error: {}", e));

            // ── Push notification ─────────────────────────────────────────────
            let token = sqlx::query_scalar!(
                "SELECT device_token FROM accounts WHERE id = $1",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .flatten();


            if let Some(token) = token {
                let (title, body) = if vas_status == "SUCCESS" {
                    (
                        "Data Purchase Successful",
                        format!(
                            "Dear valued customer, your data purchase of ₦{} ({}) for {} has been completed successfully.",
                            formatted_amount,
                            res["data"]["dataPlan"]
                                .as_str()
                                .unwrap_or(&event.product_id),
                            event.phone_number,
                        ),
                    )
                } else {
                    (
                        "Data Purchase Unsuccessful",
                        format!(
                            "Dear valued customer, we were unable to process your data purchase of ₦{} for {}. Please try again or contact support. Reference: {}.",
                            formatted_amount, event.phone_number, event.reference
                        ),
                    )
                };

                send_push_notification(
                    &token,
                    title,
                    &body,
                    Some(serde_json::json!({
                        "route":     "NotificationDetail",
                        "service":   "data_purchase",
                        "status":    tx_status,
                        "amount":    formatted_amount,
                        "reference": event.reference,
                        "recipient": event.phone_number,
                        "network":   event.network,
                    })),
                )
                .await;
            }

            
        }
        Err(e) => {

            // ── Save failed attempt ───────────────────────────────────────────
            let _ = sqlx::query!(
                r#"
                INSERT INTO vas_transactions
                    (account_id, vas_type, network, recipient, amount, currency,
                     reference, debit_account, status, response_message)
                VALUES
                    ($1, 'data', $2, $3, $4, 'NGN', $5, $6, 'failed', $7)
                ON CONFLICT (reference) DO NOTHING
                "#,
                account_uuid,
                event.network,
                event.phone_number,
                amount_bd,
                event.reference,
                event.account_number,
                e.to_string(),
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/data_purchase] DB failed insert error: {}", e));

            // ── Push notification — failed ─────────────────────────────────────
            let token = sqlx::query_scalar!(
                "SELECT device_token FROM accounts WHERE id = $1",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten()
            .flatten();

            if let Some(token) = token {
                send_push_notification(
        &token,
        "Data Purchase Unsuccessful",
        &format!(
            "Dear valued customer, we were unable to process your data purchase of ₦{} for {}. Please try again or contact support. Reference: {}.",
            formatted_amount, event.phone_number, event.reference
        ),
        Some(serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "data_purchase",
            "status":    "failed",
            "amount":    formatted_amount,
            "reference": event.reference,
            "recipient": event.phone_number,
            "network":   event.network,
        })),
    )
    .await;
            }
        }
    }
}

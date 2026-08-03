use crate::config::Config;
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
    InboundTransferEvent, KycUpgradeStatusEvent, NewDeviceLoginEvent, SetPaymentPinEvent,
    SignupEvent, SuspiciousLoginEvent, Tier2UpgradeEvent, Tier3UpgradeEvent, UpdateDeviceIdEvent,
    VerifyEmailEvent,
};
use base64::Engine;
use rand::{Rng, distributions::Alphanumeric};
use redis::aio::ConnectionManager;
use serde::Deserialize;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

pub async fn handle_signup(
    payload: &str,
    db: &PgPool,
    _cfg: &Config,
    redis: &redis::aio::ConnectionManager,
    mailer: &Mailer,
) {
    let event: SignupEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(_e) => {
            return;
        }
    };

    let random_str: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect::<String>()
        .to_uppercase();

    let referral_code = format!("PRS-{}", random_str);

    // ── 1. Check if email already exists ─────────────────────────────────────
    let existing = sqlx::query!(
        "SELECT id, email_verified FROM accounts WHERE email = $1",
        event.email
    )
    .fetch_optional(db)
    .await;

    // account_id is set in one of two ways below and used for the referral insert
    let account_id: Uuid;

    match existing {
        Ok(Some(row)) => {
            if row.email_verified {
                return;
            }
            // Account exists but not verified — skip re-creating it,
            // but still need the real id for the referral insert below
            account_id = row.id;
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

            let new_id = Uuid::new_v4();

            // ── 3. Insert account — KYC identity fields are Option and stay
            //    NULL until Dojah verification resolves them later. ──────────
            let result = sqlx::query!(
                r#"
                INSERT INTO accounts (
                    id, email, password_hash, device_id, account_type,
                    firstname, lastname, othername, phone_number,
                    date_of_birth, gender, nin, bvn, address,
                    city, lga, state, user_photo,
                    kyc_reference,
                    current_tier,
                    status, email_verified, referral_code,
                    referred_by
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13, $14,
                    $15, $16, $17, $18,
                    $19,
                    0,
                    'active', false, $20, $21
                )
                ON CONFLICT (email) DO NOTHING
                RETURNING id
                "#,
                new_id,
                event.email,
                password_hash,
                event.device_id,
                event.account_type,
                event.firstname.as_deref(),
                event.lastname.as_deref(),
                event.middlename.as_deref(),
                event.mobile_number.as_deref(),
                event.date_of_birth.as_deref(),
                event.gender.as_deref(),
                event.nin.as_deref(),
                event.bvn.as_deref(),
                event.address.as_deref(),
                event.city.as_deref(),
                event.lga.as_deref(),
                event.state.as_deref(),
                event.user_photo.as_deref(),
                event.kyc_reference,
                referral_code,
                event
                    .referrer_id
                    .as_deref()
                    .and_then(|s| Uuid::parse_str(s).ok())
            )
            .fetch_optional(db)
            .await;

            match result {
                Ok(Some(row)) => {
                    account_id = row.id;
                    println!(
                        "[worker/signup] Account created — email: {}, kyc_reference: {}",
                        event.email, event.kyc_reference
                    );
                }
                Ok(None) => {
                    match sqlx::query_scalar!(
                        "SELECT id FROM accounts WHERE email = $1",
                        event.email
                    )
                    .fetch_one(db)
                    .await
                    {
                        Ok(id) => {
                            account_id = id;
                        }
                        Err(e) => {
                            eprintln!("[worker/signup] failed to fetch existing account id: {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    println!("[worker/signup] DB insert error: {}", e);
                    return;
                }
            }
        }
    }

    // ── 4. Insert referral row if referred ───────────────────────────────────
    if let Some(referrer_id_str) = &event.referrer_id {
        if let Ok(referrer_uuid) = Uuid::parse_str(referrer_id_str) {
            if let Err(e) = sqlx::query!(
                r#"
                INSERT INTO referrals (referrer_id, referred_id, status)
                VALUES ($1, $2, 'pending')
                ON CONFLICT (referred_id) DO NOTHING
                "#,
                referrer_uuid,
                account_id
            )
            .execute(db)
            .await
            {
                eprintln!("[worker/signup] referral insert error: {}", e);
            } else {
                eprintln!(
                    "[worker/signup] referral recorded — referrer: {}, referred: {}",
                    referrer_uuid, account_id
                );
            }
        }
    }

    // ── 5. Generate + hash OTP ────────────────────────────────────────────────
    let otp = generate_otp();

    let otp_hash = match hash_password(&otp) {
        Ok(h) => h,
        Err(e) => {
            println!("[worker/signup] OTP hash error: {}", e);
            return;
        }
    };

    // ── 6. Store hashed OTP in Redis — TTL 10 minutes ────────────────────────
    let mut redis_conn = redis.clone();
    match redis::cmd("SETEX")
        .arg(&event.otp_redis_key)
        .arg(600u64)
        .arg(&otp_hash)
        .query_async::<_, ()>(&mut redis_conn)
        .await
    {
        Ok(_) => {
            println!(
                "[worker/signup] OTP stored in Redis: {}",
                event.otp_redis_key
            );
        }
        Err(e) => {
            println!("[worker/signup] Redis OTP save failed: {}", e);
            return;
        }
    }

    // ── 7. Send OTP email ─────────────────────────────────────────────────────
    // firstname isn't known yet at this stage (resolved later via KYC),
    // so fall back to a generic greeting if it's still None.
    let greeting_name = event.firstname.as_deref().unwrap_or("there");

    if let Err(e) = mailer.send_otp(&event.email, greeting_name, &otp).await {
        println!("[worker/signup] OTP email send failed: {}", e);
    } else {
        println!("[worker/signup] OTP email sent to: {}", event.email);
    }
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

pub async fn advance_referral_vas(db: &PgPool, account_uuid: uuid::Uuid) {
    let result = sqlx::query!(
        r#"
        UPDATE referrals
        SET
            status     = CASE
                             WHEN status = 'palm_done' THEN 'qualified'
                             WHEN status = 'pending'   THEN 'disqualified'
                             ELSE status
                         END,
            vas_payment_at = CASE
                                 WHEN status = 'palm_done' AND vas_payment_at IS NULL THEN NOW()
                                 ELSE vas_payment_at
                             END,
            qualified_at = CASE
                               WHEN status = 'palm_done' AND vas_payment_at IS NULL THEN NOW()
                               ELSE qualified_at
                           END,
            disqualified_reason = CASE
                                      WHEN status = 'pending' THEN 'vas_before_palm'
                                      ELSE disqualified_reason
                                  END,
            updated_at = NOW()
        WHERE referred_id = $1
          AND status NOT IN ('disqualified', 'qualified')
        "#,
        account_uuid
    )
    .execute(db)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            // check if they just qualified (not disqualified)
            let status = sqlx::query_scalar!(
                "SELECT status FROM referrals WHERE referred_id = $1",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .ok()
            .flatten();

            if status.as_deref() == Some("qualified") {
                log::info!("[referral] qualified — referred_id={}", account_uuid);
                check_and_create_reward(db, account_uuid).await;
            } else {
                log::warn!(
                    "[referral] disqualified (vas before palm) — referred_id={}",
                    account_uuid
                );
            }
        }
        Ok(_) => {}
        Err(e) => log::error!(
            "[referral] vas upsert failed for referred_id={}: {}",
            account_uuid,
            e
        ),
    }
}

async fn check_and_create_reward(db: &PgPool, account_uuid: uuid::Uuid) {
    // Get this user's referrer
    let referrer_id = match sqlx::query_scalar!(
        "SELECT referrer_id FROM referrals WHERE referred_id = $1",
        account_uuid
    )
    .fetch_optional(db)
    .await
    {
        Ok(Some(id)) => id,
        _ => {
            return;
        }
    };

    // Count unrewarded qualified referrals in batches of 5
    let count = match sqlx::query_scalar!(
        r#"
        SELECT COUNT(*) FROM referrals
        WHERE referrer_id = $1
          AND status      = 'qualified'
          AND rewarded    = false
        "#,
        referrer_id
    )
    .fetch_one(db)
    .await
    {
        Ok(Some(n)) => n,
        _ => {
            return;
        }
    };

    if count < 5 {
        return;
    }

    // Mark exactly 5 as rewarded and insert reward row atomically
    let mut tx = match db.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!("[referral] failed to begin reward tx: {}", e);
            return;
        }
    };

    // Lock and mark exactly 5 rows
    let marked = sqlx::query!(
        r#"
        UPDATE referrals
        SET rewarded    = true,
            rewarded_at = NOW(),
            updated_at  = NOW()
        WHERE id IN (
            SELECT id FROM referrals
            WHERE referrer_id = $1
              AND status      = 'qualified'
              AND rewarded    = false
            ORDER BY qualified_at ASC
            LIMIT 5
            FOR UPDATE SKIP LOCKED
        )
        "#,
        referrer_id
    )
    .execute(&mut *tx)
    .await;

    match marked {
        Ok(r) if r.rows_affected() == 5 => {}
        Ok(r) => {
            // Race — another worker got some of them, abort
            log::warn!(
                "[referral] reward race detected — rows_affected={}, rolling back",
                r.rows_affected()
            );
            let _ = tx.rollback().await;
            return;
        }
        Err(e) => {
            log::error!("[referral] reward mark failed: {}", e);
            let _ = tx.rollback().await;
            return;
        }
    }

    // Insert reward row
    if let Err(e) = sqlx::query!(
        r#"
        INSERT INTO referral_rewards (referrer_id, amount, currency, referral_count, status)
        VALUES ($1, 1000, 'NGN', 5, 'pending')
        "#,
        referrer_id
    )
    .execute(&mut *tx)
    .await
    {
        log::error!("[referral] reward insert failed: {}", e);
        let _ = tx.rollback().await;
        return;
    }

    match tx.commit().await {
        Ok(_) => log::info!(
            "[referral] ₦1000 reward queued for referrer={}",
            referrer_id
        ),
        Err(e) => log::error!("[referral] reward commit failed: {}", e),
    }
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

/// Sends the welcome email — extracted so every early-return path above still fires it.
async fn finish_verify_email(event: &VerifyEmailEvent, mailer: &Mailer) {
    if let Err(e) = mailer.send_welcome(&event.email, "").await {
        println!("[worker/verify_email] Welcome email error: {}", e);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn generate_otp() -> String {
    let mut rng = rand::thread_rng();
    format!("{:06}", rng.gen_range(100000..999999))
}

/// Fetches all active device tokens for an account from the `device_tokens` table
/// (an account can now have more than one registered device).
///
/// `DISTINCT` is used because the table's uniqueness constraint is on
/// `(account_id, device_id)`, not on `device_token` itself — so the same
/// physical device can end up with more than one row under the same account
/// (e.g. app reinstall generating a new `device_id` for a token that's
/// otherwise unchanged, or the account simply being logged into the same
/// device twice). Without deduping here, that would cause the same push
/// notification to be sent twice to the same device.
async fn fetch_device_tokens(db: &PgPool, account_uuid: uuid::Uuid) -> Vec<String> {
    match sqlx::query_scalar!(
        "SELECT DISTINCT device_token FROM device_tokens WHERE account_id = $1",
        account_uuid
    )
    .fetch_all(db)
    .await
    {
        Ok(tokens) => tokens,
        Err(e) => {
            log::error!(
                "[device_tokens] failed to fetch tokens for account_id={}: {}",
                account_uuid,
                e
            );
            Vec::new()
        }
    }
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
            account_uuid
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
        account_uuid
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
        account_uuid
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
            account_uuid
        )
        .execute(db),
        sqlx::query!(
            r#"INSERT INTO account_contact_history
       (account_id, field, old_value, new_value, changed_at)
       VALUES ($1, 'email', $2, $3, NOW())
       ON CONFLICT (account_id, field, old_value, new_value) DO NOTHING"#,
            account_uuid,
            event.old_email,
            event.new_email
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

pub async fn handle_tier2_upgrade(
    payload: &str,
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let event: Tier2UpgradeEvent = match serde_json::from_str(payload) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[worker/tier2_upgrade] Invalid payload: {}", e);
            return;
        }
    };

    let account_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(_) => {
            eprintln!("[worker/tier2_upgrade] Invalid account_id");
            return;
        }
    };

    // ── Fetch account ─────────────────────────────────────────────────────────
    let row = match
        sqlx
            ::query!(
                "SELECT current_tier, status, pending_tier_upgrade, email, account_number, account_name FROM accounts WHERE id = $1 AND deleted_at IS NULL",
                account_uuid
            )
            .fetch_optional(db).await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            eprintln!("[worker/tier2_upgrade] Account not found: {}", event.account_id);
            return;
        }
        Err(e) => {
            eprintln!("[worker/tier2_upgrade] DB error: {}", e);
            return;
        }
    };

    if row.status != "active" {
        eprintln!(
            "[worker/tier2_upgrade] Account not active: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier < 1 {
        eprintln!(
            "[worker/tier2_upgrade] Must be tier 1 before upgrading to tier 2: {}",
            event.account_id
        );
        return;
    }

    if row.current_tier >= 2 {
        eprintln!(
            "[worker/tier2_upgrade] Already tier 2+: {}",
            event.account_id
        );
        return;
    }

    if row.pending_tier_upgrade.is_some() {
        eprintln!(
            "[worker/tier2_upgrade] Upgrade already in progress: {}",
            event.account_id
        );
        return;
    }

    // ── Must have account number from tier 1 ──────────────────────────────────
    let account_number = match row.account_number.clone() {
        Some(n) => n,
        None => {
            eprintln!(
                "[worker/tier2_upgrade] No account number — must complete tier 1 first: {}",
                event.account_id
            );
            return;
        }
    };

    let account_name = row.account_name.clone().unwrap_or_default();

    // ── Resolve userPhoto — fetch URL and convert to base64 if needed ─────────
    let user_photo_base64 = if event.user_photo.starts_with("http") {
        eprintln!(
            "[worker/tier2_upgrade] userPhoto is a URL, fetching and converting to base64..."
        );
        match reqwest::get(&event.user_photo).await {
            Ok(resp) => match resp.bytes().await {
                Ok(bytes) => {
                    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                    eprintln!(
                        "[worker/tier2_upgrade] userPhoto converted, base64 len: {}",
                        b64.len()
                    );
                    b64
                }
                Err(e) => {
                    eprintln!(
                        "[worker/tier2_upgrade] Failed to read userPhoto bytes: {}",
                        e
                    );
                    event.user_photo.clone()
                }
            },
            Err(e) => {
                eprintln!(
                    "[worker/tier2_upgrade] Failed to fetch userPhoto URL: {}",
                    e
                );
                event.user_photo.clone()
            }
        }
    } else {
        // Already base64
        event.user_photo.clone()
    };

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
        eprintln!("[worker/tier2_upgrade] Failed to set pending: {}", e);
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
        "userPhoto": user_photo_base64,
        "idType": event.id_type.to_string(),
        "idNumber": event.id_number,
        "idCardFront": event.id_card_front,
        "houseNumber": event.house_number,
        "streetName": event.street_name,
        "state": event.state,
        "city": event.city,
        "localGovernment": event.local_government,
        "pep": event.pep,
        "customerSignature": event.customer_signature,
        "utilityBill": event.utility_bill,
        "nearestLandmark": event.nearest_landmark,
    });

    // ── Log all fields before sending ─────────────────────────────────────────
    eprintln!("[worker/tier2_upgrade] ── outgoing payload ──────────────────");
    eprintln!("  accountNumber:          {}", account_number);
    eprintln!("  bvn:                    {}", event.bvn);
    eprintln!("  nin:                    {}", event.nin);
    eprintln!("  accountName:            {}", account_name);
    eprintln!("  phoneNumber:            {}", event.phone_no);
    eprintln!("  email:                  {}", row.email);
    eprintln!("  idType:                 {}", event.id_type);
    eprintln!("  idNumber:               {}", event.id_number);
    eprintln!("  pep:                    {}", event.pep);
    eprintln!("  houseNumber:            {}", event.house_number);
    eprintln!("  streetName:             {}", event.street_name);
    eprintln!("  state:                  {}", event.state);
    eprintln!("  city:                   {}", event.city);
    eprintln!("  localGovernment:        {}", event.local_government);
    eprintln!("  nearestLandmark:        {}", event.nearest_landmark);
    eprintln!("  userPhoto len:          {}", user_photo_base64.len());
    eprintln!("  idCardFront len:        {}", event.id_card_front.len());
    eprintln!(
        "  customerSignature len:  {}",
        event.customer_signature.len()
    );
    eprintln!("  utilityBill len:        {}", event.utility_bill.len());
    eprintln!("[worker/tier2_upgrade] ─────────────────────────────────────");

    match psb
        .post(redis, "/waas/api/v1/wallet_upgrade", &upgrade_body)
        .await
    {
        Ok(json) => {
            eprintln!("[worker/tier2_upgrade] wallet_upgrade response: {:?}", json);

            let is_success =
                json["status"].as_str().map(|s| s.to_uppercase()) == Some("SUCCESS".to_string());

            if is_success {
                eprintln!(
                    "[worker/tier2_upgrade] Submitted successfully, awaiting webhook: {}",
                    row.email
                );
            } else {
                eprintln!("[worker/tier2_upgrade] wallet_upgrade rejected: {:?}", json);
                // clear pending so user can retry
                let _ = sqlx
                    ::query!(
                        "UPDATE accounts SET pending_tier_upgrade = NULL, updated_at = NOW() WHERE id = $1",
                        account_uuid
                    )
                    .execute(db).await;
            }
        }
        Err(e) => {
            eprintln!("[worker/tier2_upgrade] wallet_upgrade error: {}", e);
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
                let _ = sqlx
                    ::query!(
                        "UPDATE accounts SET pending_tier_upgrade = NULL, updated_at = NOW() WHERE id = $1",
                        account_uuid
                    )
                    .execute(db).await;
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

    // ── 9PSB's own bank code. Internal transfers move between two Pressend
    //    wallets, both hosted on 9PSB, so "bank" on the customer.account block
    //    is always 9PSB's own code, and the transfer is flagged as P2P rather
    //    than INTRA_BANK/OTHER_BANK (those are for the external-transfer flow). ──
    const NINE_PSB_BANK_CODE: &str = "120001";

    // ── Fetch sender + receiver display info up front — needed both for the
    //    PSB payload (name/sendername) and for the sender's push notification
    //    later. Device tokens now live in the dedicated `device_tokens` table
    //    (an account can have several), so they're fetched separately below. ──
    let (sender_info, reciever_info) = tokio::join!(
        sqlx::query!(
            "SELECT firstname, lastname FROM accounts WHERE id = $1",
            sender_uuid
        )
        .fetch_optional(db),
        sqlx::query!(
            "SELECT firstname, lastname FROM accounts WHERE id = $1",
            reciever_uuid
        )
        .fetch_optional(db)
    );

    let sender_device_tokens = fetch_device_tokens(db, sender_uuid).await;

    let sender_display_name = match &sender_info {
        Ok(Some(row)) => {
            let name = format!(
                "{} {}",
                row.firstname.as_deref().unwrap_or(""),
                row.lastname.as_deref().unwrap_or("")
            )
            .trim()
            .to_string();
            if name.is_empty() {
                event.sender_account.clone()
            } else {
                name
            }
        }
        _ => event.sender_account.clone(),
    };

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

    // ── Call 9PSB wallet_other_banks — P2P transfer between two Pressend
    //    wallets. Replaces the previous separate /debit/transfer +
    //    /credit/transfer calls with a single atomic PSB call. ────────────────
    let psb_payload = serde_json::json!({
        "customer": {
            "account": {
                "bank":                NINE_PSB_BANK_CODE,
                "name":                recipient_display_name,
                "number":              event.recipient_account,
                "senderaccountnumber": event.sender_account,
                "sendername":          sender_display_name,
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
        "transactionType": "P2P",
        "merchant": {
            "isFee":              false,
            "merchantFeeAccount": "",
            "merchantFeeAmount":  ""
        }
    });

    let psb_res = match PsbClient::new(cfg)
        .post(redis, "/waas/api/v1/wallet_other_banks", &psb_payload)
        .await
    {
        Ok(res) => res,
        Err(e) => {
            println!(
                "[worker/internal_transfer] PSB error — ref: {}, error: {}",
                event.reference, e
            );
            return;
        }
    };

    println!("[worker/internal_transfer] PSB response: {:?}", psb_res);

    let status = psb_res["status"].as_str().unwrap_or("").to_uppercase();

    if status != "SUCCESS" {
        // ── PSB call failed as a single atomic operation — record both ledger
        //    legs as failed together and notify the sender. ───────────────────
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
                "failed"
            ),
            insert_transaction(
                db,
                sender_uuid,
                reciever_uuid,
                &failed_credit_ref,
                "credit",
                &event.amount,
                &event.narration,
                "failed"
            )
        );

        for token in &sender_device_tokens {
            send_push_notification(
                token,
                "Pressend Transfer Issue",
                &format!(
                    "Your transfer of ₦{} could not be completed. You will be refunded.",
                    &event.amount
                ),
                Some(serde_json::json!({
                    "route":     "NotificationDetail",
                    "service":   "transfer_sent",
                    "status":    "failed",
                    "amount":    &event.amount,
                    "reference": event.reference,
                    "narration": "Transfer could not be completed. Refund in progress.",
                })),
            )
            .await;
        }

        println!(
            "[worker/internal_transfer] ── Failed — ref: {}, status: '{}' ──",
            event.reference, status
        );
        return;
    }

    // ── Success — insert DB ledger records for both legs concurrently ────────
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
            "success"
        ),
        insert_transaction(
            db,
            sender_uuid,
            reciever_uuid,
            &success_credit_ref,
            "credit",
            &event.amount,
            &event.narration,
            "success"
        )
    );

    if let Err(e) = debit_result {
        println!("[worker/internal_transfer] Debit DB insert error: {}", e);
    }
    if let Err(e) = credit_result {
        println!("[worker/internal_transfer] Credit DB insert error: {}", e);
    }

    // ── Track sender's daily transaction total in Redis ───────────────────────
    let amount_f64 = event.amount.parse::<f64>().unwrap_or(0.0);
    let today = chrono::Utc::now().format("%Y-%m-%d");
    let daily_key = format!("daily_txn_total:{}:{}", sender_uuid, today);
    let mut redis_daily = redis.clone();

    let new_total: Result<f64, redis::RedisError> = redis::cmd("INCRBYFLOAT")
        .arg(&daily_key)
        .arg(amount_f64)
        .query_async(&mut redis_daily)
        .await;

    match new_total {
        Ok(total) => {
            let _: Result<(), _> = redis::cmd("EXPIRE")
                .arg(&daily_key)
                .arg(86400i64)
                .query_async(&mut redis_daily)
                .await;
            log::info!(
                "[worker/internal_transfer] daily total for {} is now {}",
                sender_uuid,
                total
            );
        }
        Err(e) => {
            log::error!(
                "[worker/internal_transfer] failed to update daily total for {}: {}",
                sender_uuid,
                e
            );
        }
    }

    // ── Push notification — sender only. Receiver credit alert is handled by
    //    the PSB inbound webhook (handle_transfer_inflow), same as before. ────
    for token in &sender_device_tokens {
        send_push_notification(
            token,
            "Transfer Successful",
            &format!(
                "₦{} sent successfully to {}",
                &event.amount, recipient_display_name
            ),
            Some(serde_json::json!({
                "route":     "NotificationDetail",
                "service":   "transfer_sent",
                "status":    "success",
                "amount":    &event.amount,
                "reference": event.reference,
                "recipient": recipient_display_name,
            })),
        )
        .await;
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

    let amount_clean = event.amount.replace(",", "");
    let amount_bd = bigdecimal::BigDecimal::from_str(&amount_clean).unwrap_or_default();
    let amount_f64: f64 = amount_clean.parse().unwrap_or(0.0);
    let is_above_10k = amount_f64 >= 10_000.0;

    // ── Check if transactionref already exists in our system ─────────────────
    let existing = sqlx::query!(
        "SELECT id, status FROM transactions WHERE reference = $1",
        event.transaction_ref
    )
    .fetch_optional(db)
    .await
    .unwrap_or(None);

    if let Some(tx) = existing {
        let status_str = event.status.as_deref().unwrap_or("").to_lowercase();
        let new_status = if status_str == "failed" {
            "failed"
        } else {
            "success"
        };

        // ── Idempotency check — if status hasn't changed, webhook fired twice ─
        if tx.status == new_status {
            println!(
                "[worker/transfer_inflow] Duplicate webhook — status unchanged, skipping — ref: {}",
                event.transaction_ref
            );
            return;
        }

        // ── Status actually changed — update only, no notification ────────────
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
            tx.id
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
    let receiver = match sqlx::query!(
        r#"
        SELECT id, account_type, account_name
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
            $1, $2, $3, 'credit', $4, 'NGN', $5, 'success', 'external',
            $6, $7, '9PSB', $8
        )
        ON CONFLICT (reference) DO NOTHING
        "#,
        None as Option<uuid::Uuid>,
        Some(receiver.id) as Option<uuid::Uuid>,
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
        })
    )
    .execute(db)
    .await;

    match result {
        Ok(r) if r.rows_affected() == 0 => {
            // Another worker beat us to it — do not notify
            println!(
                "[worker/transfer_inflow] Duplicate — already inserted — ref: {}",
                event.transaction_ref
            );
            return;
        }
        Ok(_) => println!(
            "[worker/transfer_inflow] External inflow recorded — ref: {}, account: {}, amount: ₦{}",
            event.transaction_ref, event.account_number, &event.amount
        ),
        Err(e) => {
            println!("[worker/transfer_inflow] DB insert error: {}", e);
            return;
        }
    }

    // ── Fetch device tokens for the receiver up front — used by both the fee
    //    notification and the credit alert below. ─────────────────────────────
    let receiver_device_tokens = fetch_device_tokens(db, receiver.id).await;

    // ── Debit ₦35 fee if business account and amount > ₦10,000 ───────────────
    if should_charge_fee {
        let fee_ref = format!("{}-FEE", event.transaction_ref);

        // ── Guard: skip entirely if fee was already processed (prevents double
        //    notification when the webhook is delivered more than once) ─────────
        let fee_exists = sqlx::query!("SELECT id FROM transactions WHERE reference = $1", fee_ref)
            .fetch_optional(db)
            .await
            .unwrap_or(None);

        if fee_exists.is_some() {
            println!(
                "[worker/transfer_inflow] Fee already processed — skipping — ref: {}",
                fee_ref
            );
        } else {
            let receiver_name = receiver.account_name.clone().unwrap_or_default();

            let fee_payload = serde_json::json!({
                "accountNo":     event.account_number,
                "narration":     "Incoming transfer processing fee",
                "totalAmount":   "1",
                "transactionId": fee_ref,
                "merchant": {
                    "isFee":              true,
                    "merchantFeeAccount": &cfg._9psb_operational_account,
                    "merchantFeeAmount":  "34"
                }
            });

            match PsbClient::new(cfg)
                .post(redis, "/waas/api/v1/debit/transfer", &fee_payload)
                .await
            {
                Ok(res) => {
                    let status = res["status"].as_str().unwrap_or("").to_uppercase();
                    if status == "SUCCESS" {
                        println!("[worker/transfer_inflow] Fee deducted — ref: {}", fee_ref);

                        let fee_bd = bigdecimal::BigDecimal::from_str("35").unwrap_or_default();
                        let fee_narration = format!(
                            "Processed fee for ₦35 of {} credit alert greater than 10,000",
                            receiver_name
                        );

                        let _: Result<_, _> = sqlx::query!(
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
                                meta
                            )
                            VALUES ($1, NULL, $2, 'debit', $3, 'NGN',
                                    $4, 'success', 'internal', $5)
                            ON CONFLICT (reference) DO NOTHING
                            "#,
                            Some(receiver.id) as Option<uuid::Uuid>,
                            fee_ref,
                            fee_bd,
                            fee_narration,
                            serde_json::json!({
                                "fee_type":   "incoming_transfer_fee",
                                "linked_ref": event.transaction_ref,
                                "fee_amount": "35.00",
                            })
                        )
                        .execute(db)
                        .await
                        .map_err(|e| {
                            println!("[worker/transfer_inflow] Fee DB insert error: {}", e)
                        });

                        // ── Notify business owner about fee ────────────────────
                        for token in &receiver_device_tokens {
                            send_push_notification(
                                token,
                                "Processing Fee Deducted",
                                &format!(
                                    "A processing fee of ₦35.00 has been deducted from your \
                                     account for an incoming transfer of ₦{} from {} ({}).",
                                    &event.amount, event.sender_name, event.sender_bank
                                ),
                                Some(serde_json::json!({
                                    "route":     "NotificationDetail",
                                    "service":   "transfer_fee",
                                    "status":    "success",
                                    "amount":    "35.00",
                                    "reference": fee_ref,
                                    "narration": format!(
                                        "Processing fee for ₦{} inflow from {} · {}",
                                        &event.amount, event.sender_name, event.sender_bank
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
    }

    // ── Credit alert push notification (fires exactly once — on first insert) ──
    let (title, body) = if should_charge_fee {
        (
            "Credit Alert",
            format!(
                "Your wallet has been credited with ₦{}. \
                 A processing fee of ₦35.00 was applied as this transfer exceeds ₦10,000. \
                 Sent by: {} ({}).",
                &event.amount, event.sender_name, event.sender_bank
            ),
        )
    } else {
        (
            "Credit Alert",
            format!(
                "Your wallet has been credited with ₦{}. \
                 Sent by: {} ({}).",
                &event.amount, event.sender_name, event.sender_bank
            ),
        )
    };

    for token in &receiver_device_tokens {
        send_push_notification(
            token,
            title,
            &body,
            Some(serde_json::json!({
                "route":     "NotificationDetail",
                "service":   "transfer_received",
                "status":    "success",
                "amount":    &event.amount,
                "reference": event.transaction_ref,
                "sender":    event.sender_name,
                "narration": format!("From {} · {}", event.sender_name, event.sender_bank),
            })),
        )
        .await;
    }

    println!(
        "[worker/transfer_inflow] ── Complete — ref: {}, account: {}, amount: ₦{}, fee_charged: {} ──",
        event.transaction_ref, event.account_number, &event.amount, should_charge_fee
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
        status
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

    let sender_uuid = match uuid::Uuid::parse_str(&event.account_id) {
        Ok(id) => id,
        Err(e) => {
            println!("[worker/external_transfer] Invalid sender uuid: {}", e);
            return;
        }
    };

    // ── 9PSB's own bank code. When the recipient's bank_code matches this,
    //    the transfer stays entirely within 9PSB's rails and must be flagged
    //    as INTRA_BANK. Every other bank code is a real inter-bank transfer,
    //    which 9PSB treats as OTHER_BANK (their documented default). ─────────
    const NINE_PSB_BANK_CODE: &str = "120001";

    let transaction_type = if event.bank_code == NINE_PSB_BANK_CODE {
        "INTRA_BANK"
    } else {
        "OTHER_BANK"
    };

    println!(
        "[worker/external_transfer] resolved transactionType={} for bank_code={} — ref: {}",
        transaction_type, event.bank_code, event.reference
    );

    // ── Format amount once for reuse across notifications ─────────────────────
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
        "transactionType": transaction_type,
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
                        &event.amount, event.recipient_name, event.reference
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
                        &event.amount, event.recipient_name, event.reference
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
                    "transaction_type": transaction_type,
                    "psb_response_code": res["responseCode"].as_str().unwrap_or(""),
                })
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/external_transfer] DB insert error: {}", e));

            // ── Track sender's daily transaction total in Redis ───────────────────────
            let amount_f64 = event.amount.parse::<f64>().unwrap_or(0.0);
            let today = chrono::Utc::now().format("%Y-%m-%d");
            let daily_key = format!("daily_txn_total:{}:{}", sender_uuid, today);
            let mut redis_daily = redis.clone();

            let new_total: Result<f64, redis::RedisError> = redis::cmd("INCRBYFLOAT")
                .arg(&daily_key)
                .arg(amount_f64)
                .query_async(&mut redis_daily)
                .await;

            match new_total {
                Ok(total) => {
                    let _: Result<(), _> = redis::cmd("EXPIRE")
                        .arg(&daily_key)
                        .arg(86400i64)
                        .query_async(&mut redis_daily)
                        .await;
                    log::info!(
                        "[worker/external_transfer] daily total for {} is now {}",
                        sender_uuid,
                        total
                    );
                }
                Err(e) => {
                    log::error!(
                        "[worker/external_transfer] failed to update daily total for {}: {}",
                        sender_uuid,
                        e
                    );
                }
            }

            // ── Push notification ─────────────────────────────────────────────
            let tokens = fetch_device_tokens(db, sender_uuid).await;

            for token in &tokens {
                send_push_notification(
                    token,
                    push_title,
                    &push_body,
                    Some(
                        serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "external_transfer",
            "status":    if status == "SUCCESS" { "success" } else { "failed" },
            "amount":    &event.amount,
            "reference": event.reference,
            "recipient": event.recipient_name,
            "narration": format!("{} · {}", event.recipient_name, event.recipient_number),
        })
                    )
                ).await;
            }

            println!(
                "[worker/external_transfer] ── Complete — ref: {}, status: {}, transactionType: {} ──",
                event.reference, tx_status, transaction_type
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
                })
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/airtime_purchase] DB insert error: {}", e));

            // ── Referral: advance VAS milestone ──────────────────────────────────────────
            if tx_status == "success" {
                let db_clone = db.clone();
                tokio::spawn(async move {
                    advance_referral_vas(&db_clone, account_uuid).await;
                });
            }

            // ── Push notification ─────────────────────────────────────────────
            let tokens = fetch_device_tokens(db, account_uuid).await;

            if !tokens.is_empty() {
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

                for token in &tokens {
                    send_push_notification(
                        token,
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
                e.to_string()
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/airtime_purchase] DB failed insert error: {}", e));

            // ── Push notification — failed ─────────────────────────────────────
            let tokens = fetch_device_tokens(db, account_uuid).await;

            for token in &tokens {
                send_push_notification(
                    token,
                    "Airtime Purchase Unsuccessful",
                    &format!(
                        "Dear valued customer, we were unable to process your airtime recharge of ₦{} to {}. Please try again or contact support. Reference: {}.",
                        event.amount,
                        event.phone_number,
                        event.reference
                    ),
                    Some(
                        serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "airtime_purchase",
            "status":    "failed",
            "amount":    event.amount,
            "reference": event.reference,
            "recipient": event.phone_number,
            "network":   event.network,
        })
                    )
                ).await;
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
                })
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/data_purchase] DB insert error: {}", e));

            // ── Referral: advance VAS milestone ──────────────────────────────────────────
            if tx_status == "success" {
                let db_clone = db.clone();
                tokio::spawn(async move {
                    advance_referral_vas(&db_clone, account_uuid).await;
                });
            }
            // ── Push notification ─────────────────────────────────────────────
            let tokens = fetch_device_tokens(db, account_uuid).await;

            if !tokens.is_empty() {
                let (title, body) = if vas_status == "SUCCESS" {
                    (
                        "Data Purchase Successful",
                        format!(
                            "Dear valued customer, your data purchase of ₦{} ({}) for {} has been completed successfully.",
                            &event.amount,
                            res["data"]["dataPlan"]
                                .as_str()
                                .unwrap_or(&event.product_id),
                            event.phone_number
                        ),
                    )
                } else {
                    (
                        "Data Purchase Unsuccessful",
                        format!(
                            "Dear valued customer, we were unable to process your data purchase of ₦{} for {}. Please try again or contact support. Reference: {}.",
                            &event.amount, event.phone_number, event.reference
                        ),
                    )
                };

                for token in &tokens {
                    send_push_notification(
                        token,
                        title,
                        &body,
                        Some(serde_json::json!({
                            "route":     "NotificationDetail",
                            "service":   "data_purchase",
                            "status":    tx_status,
                            "amount":    &event.amount,
                            "reference": event.reference,
                            "recipient": event.phone_number,
                            "network":   event.network,
                        })),
                    )
                    .await;
                }
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
                e.to_string()
            )
            .execute(db)
            .await
            .map_err(|e| println!("[worker/data_purchase] DB failed insert error: {}", e));

            // ── Push notification — failed ─────────────────────────────────────
            let tokens = fetch_device_tokens(db, account_uuid).await;

            for token in &tokens {
                send_push_notification(
                    token,
                    "Data Purchase Unsuccessful",
                    &format!(
                        "Dear valued customer, we were unable to process your data purchase of ₦{} for {}. Please try again or contact support. Reference: {}.",
                        &event.amount,
                        event.phone_number,
                        event.reference
                    ),
                    Some(
                        serde_json::json!({
            "route":     "NotificationDetail",
            "service":   "data_purchase",
            "status":    "failed",
            "amount":    &event.amount,
            "reference": event.reference,
            "recipient": event.phone_number,
            "network":   event.network,
        })
                    )
                ).await;
            }
        }
    }
}
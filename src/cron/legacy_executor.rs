use std::str::FromStr;

use bigdecimal::BigDecimal;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use tokio::time::{Duration, interval};

use crate::{
    config::Config,
    modules::legacy_plan::schemas::LegacyBeneficiarySummary,
    utils::{mailer::Mailer, psb::PsbClient},
};

pub fn spawn(db: PgPool, redis: ConnectionManager, cfg: Config) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(24 * 60 * 60));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            run_once(&db, &redis, &cfg).await;
        }
    });
}

fn build_mailer(cfg: &Config) -> Mailer {
    Mailer::new(
        &cfg.mail_server,
        cfg.mail_port,
        &cfg.mail_user,
        &cfg.mail_password,
        &cfg.app_name,
        &cfg.support_email,
    )
}

async fn run_once(db: &PgPool, redis: &ConnectionManager, cfg: &Config) {
    println!("[legacy_cron] ── Starting daily sweep ──────────────────────────");

    let plans = sqlx::query!(
        r#"
        SELECT
            lp.id,
            lp.account_id,
            lp.inactivity_days,
            lp.grace_period_days,
            lp.notify_beneficiaries,
            lp.triggered_at,
            lp.last_activity_at,
            a.account_number  AS owner_account_number,
            a.firstname       AS owner_firstname,
            a.email           AS owner_email,
            a.device_token    AS owner_device_token
        FROM legacy_plans lp
        JOIN accounts a ON a.id = lp.account_id AND a.deleted_at IS NULL
        WHERE lp.deleted_at   IS NULL
          AND lp.executed_at  IS NULL
          AND lp.status       = 'active'
          AND NOW() >= lp.last_activity_at + (lp.inactivity_days || ' days')::INTERVAL
        "#
    )
    .fetch_all(db)
    .await;

    let plans = match plans {
        Ok(p) => p,
        Err(e) => {
            println!("[legacy_cron] DB error fetching plans: {}", e);
            return;
        }
    };

    println!("[legacy_cron] Found {} triggered plan(s)", plans.len());

    for plan in plans {
        let plan_id = plan.id;
        let owner_name = plan
            .owner_firstname
            .clone()
            .unwrap_or_else(|| "Account Holder".to_string());

        // ── First trigger — start grace period ────────────────────────────────
        if plan.triggered_at.is_none() {
            println!(
                "[legacy_cron] Plan {} — first trigger, starting grace period ({} days)",
                plan_id, plan.grace_period_days
            );

            if let Err(e) = sqlx::query!(
                "UPDATE legacy_plans SET triggered_at = NOW(), updated_at = NOW() WHERE id = $1",
                plan_id
            )
            .execute(db)
            .await
            {
                println!(
                    "[legacy_cron] Failed to set triggered_at for {}: {}",
                    plan_id, e
                );
                continue;
            }

            let bene_count = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM legacy_next_of_kin WHERE legacy_id = $1",
                plan_id
            )
            .fetch_one(db)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0) as usize;

            let execution_date = (chrono::Utc::now()
                + chrono::Duration::days(plan.grace_period_days as i64))
            .format("%B %d, %Y")
            .to_string();

            // Push — owner only
            if let Some(ref token) = plan.owner_device_token {
                send_push(
                    token,
                    "⚠️ Legacy Plan Triggered",
                    &format!(
                        "Your legacy plan has been triggered due to {} days of inactivity. \
                         You have {} days to open the app and cancel before your assets \
                         are distributed.",
                        plan.inactivity_days, plan.grace_period_days
                    ),
                    Some(serde_json::json!({
                        "type":    "legacy_plan_triggered",
                        "plan_id": plan_id,
                    })),
                )
                .await;
            }

            // Email — owner only
            let email = &plan.owner_email;
            let mailer = build_mailer(cfg);
            if let Err(e) = mailer
                .send_legacy_grace_period(
                    email,
                    &owner_name,
                    plan.inactivity_days,
                    plan.grace_period_days,
                    bene_count,
                    &execution_date,
                )
                .await
            {
                println!(
                    "[legacy_cron] Grace period email error for {}: {}",
                    email, e
                );
            } else {
                println!("[legacy_cron] Grace period email sent to {}", email);
            }

            continue;
        }

        // ── Grace period check ────────────────────────────────────────────────
        let triggered_at = plan.triggered_at.unwrap();
        let grace_expires_at = triggered_at + chrono::Duration::days(plan.grace_period_days as i64);

        if chrono::Utc::now() < grace_expires_at {
            println!(
                "[legacy_cron] Plan {} — still within grace period, skipping",
                plan_id
            );
            continue;
        }

        println!(
            "[legacy_cron] Plan {} — grace period expired, executing transfers",
            plan_id
        );

        let owner_account = match &plan.owner_account_number {
            Some(acc) => acc.clone(),
            None => {
                println!(
                    "[legacy_cron] Plan {} — owner has no account number, skipping",
                    plan_id
                );
                continue;
            }
        };

        let kin_rows = sqlx::query!(
            r#"
            SELECT
                id,
                fullname,
                email,
                account_type,
                share_percentage,
                legacy_message,
                account_id,
                bank_code,
                bank_name,
                account_number,
                account_name
            FROM legacy_next_of_kin
            WHERE legacy_id = $1
            "#,
            plan_id
        )
        .fetch_all(db)
        .await;

        let kin_rows = match kin_rows {
            Ok(k) => k,
            Err(e) => {
                println!(
                    "[legacy_cron] Failed to fetch kin for plan {}: {}",
                    plan_id, e
                );
                continue;
            }
        };

        // ── Fetch wallet balance ───────────────────────────────────────────────
        let mut redis_conn = redis.clone();

        let balance_res = PsbClient::new(cfg)
            .post(
                &mut redis_conn,
                "/waas/api/v1/wallet_enquiry",
                &serde_json::json!({ "accountNo": owner_account }),
            )
            .await;

        let total_balance: f64 = match balance_res {
            Ok(ref res) => res["data"]["availableBalance"].as_f64().unwrap_or(0.0),
            Err(e) => {
                println!(
                    "[legacy_cron] Could not fetch balance for plan {}: {}",
                    plan_id, e
                );
                0.0
            }
        };

        if total_balance <= 0.0 {
            println!(
                "[legacy_cron] Plan {} — zero balance, marking executed",
                plan_id
            );
            mark_executed(db, plan_id).await;
            continue;
        }

        println!(
            "[legacy_cron] Plan {} — available balance: ₦{:.2}",
            plan_id, total_balance
        );

        let execution_date = chrono::Utc::now()
            .format("%B %d, %Y at %H:%M UTC")
            .to_string();

        let mut all_ok = true;
        let mut total_distributed = 0.0_f64;
        let mut owner_summary: Vec<LegacyBeneficiarySummary> = Vec::new();

        for kin in &kin_rows {
            let share_amount = (kin.share_percentage / 100.0) * total_balance;
            let amount_str = format!("{:.2}", share_amount);
            let amount_bd = BigDecimal::from_str(&amount_str).unwrap_or_default();

            let reference = format!(
                "LEGACY-{}-{}-{}",
                plan_id,
                kin.id,
                chrono::Utc::now().timestamp()
            );

            let narration = format!("Legacy plan distribution — {}% share", kin.share_percentage);

            let kin_display_name = kin
                .fullname
                .clone()
                .or_else(|| kin.account_name.clone())
                .unwrap_or_else(|| "Beneficiary".to_string());

            match kin.account_type.as_str() {
                // ── Internal ─────────────────────────────────────────────────
                "internal" => {
                    let recipient_account_id = match kin.account_id {
                        Some(id) => id,
                        None => {
                            println!("[legacy_cron] Kin {} internal but no account_id", kin.id);
                            all_ok = false;
                            continue;
                        }
                    };

                    let recipient_acc = sqlx::query!(
                        r#"SELECT account_number, device_token, firstname, email
                           FROM accounts WHERE id = $1 AND deleted_at IS NULL"#,
                        recipient_account_id
                    )
                    .fetch_optional(db)
                    .await;

                    let (recip_acc_num, recip_token, recip_firstname, recip_email) =
                        match recipient_acc {
                            Ok(Some(r)) => {
                                let n = match r.account_number {
                                    Some(n) => n,
                                    None => {
                                        println!(
                                            "[legacy_cron] Recipient acc num null for kin {}",
                                            kin.id
                                        );
                                        all_ok = false;
                                        continue;
                                    }
                                };
                                (n, r.device_token, r.firstname, r.email)
                            }
                            _ => {
                                println!("[legacy_cron] Recipient not found for kin {}", kin.id);
                                all_ok = false;
                                continue;
                            }
                        };

                    // 5% platform fee
                    let fee_amount = share_amount * 0.05;
                    let net_amount = share_amount - fee_amount;
                    let fee_str = format!("{:.2}", fee_amount);
                    let net_str = format!("{:.2}", net_amount);
                    let net_bd = BigDecimal::from_str(&net_str).unwrap_or_default();
                    let credit_ref = format!("{}-CR", reference);

                    // Step 1: Debit sender
                    let debit_payload = serde_json::json!({
                        "accountNo":     owner_account,
                        "narration":     narration,
                        "totalAmount":   share_amount,
                        "transactionId": reference,
                        "merchant": {
                            "isFee":              true,
                            "merchantFeeAccount": cfg._9psb_operational_account,
                            "merchantFeeAmount":  fee_str
                        }
                    });

                    let debit_ok = match PsbClient::new(cfg)
                        .post(
                            &mut redis_conn,
                            "/waas/api/v1/debit/transfer",
                            &debit_payload,
                        )
                        .await
                    {
                        Ok(res) => {
                            let st = res["status"].as_str().unwrap_or("").to_uppercase();
                            if st != "SUCCESS" {
                                println!(
                                    "[legacy_cron] Internal debit rejected — kin: {}, status: {}",
                                    kin.id, st
                                );
                                let _ = sqlx::query!(
                                    r#"INSERT INTO transactions
                                        (sender_id, reciever_id, reference, type, amount, currency,
                                         narration, status, channel, meta)
                                       VALUES ($1, $2, $3, 'debit', $4, 'NGN', $5, 'failed', 'internal', $6)
                                       ON CONFLICT (reference) DO NOTHING"#,
                                    plan.account_id, recipient_account_id, reference,
                                    amount_bd, narration,
                                    serde_json::json!({
                                        "source": "legacy_plan", "plan_id": plan_id,
                                        "kin_id": kin.id, "share_percentage": kin.share_percentage,
                                    }),
                                )
                                .execute(db)
                                .await
                                .map_err(|e| println!("[legacy_cron] TX insert error: {}", e));
                                false
                            } else {
                                true
                            }
                        }
                        Err(e) => {
                            println!("[legacy_cron] PSB debit error for kin {}: {}", kin.id, e);
                            false
                        }
                    };

                    if !debit_ok {
                        all_ok = false;
                        continue;
                    }

                    // Step 2: Credit recipient
                    let credit_payload = serde_json::json!({
                        "accountNo":     recip_acc_num,
                        "narration":     narration,
                        "totalAmount":   net_amount,
                        "transactionId": credit_ref,
                        "merchant": {
                            "isFee":              false,
                            "merchantFeeAccount": "",
                            "merchantFeeAmount":  ""
                        }
                    });

                    match PsbClient::new(cfg)
                        .post(
                            &mut redis_conn,
                            "/waas/api/v1/credit/transfer",
                            &credit_payload,
                        )
                        .await
                    {
                        Ok(res) => {
                            let st = res["status"].as_str().unwrap_or("").to_uppercase();
                            let tx_status = if st == "SUCCESS" { "success" } else { "failed" };

                            let _ = sqlx::query!(
                                r#"INSERT INTO transactions
                                    (sender_id, reciever_id, reference, type, amount, currency,
                                     narration, status, channel, meta)
                                   VALUES ($1, $2, $3, 'debit', $4, 'NGN', $5, $6, 'internal', $7)
                                   ON CONFLICT (reference) DO NOTHING"#,
                                plan.account_id,
                                recipient_account_id,
                                reference,
                                amount_bd,
                                narration,
                                tx_status,
                                serde_json::json!({
                                    "source": "legacy_plan", "plan_id": plan_id,
                                    "kin_id": kin.id, "share_percentage": kin.share_percentage,
                                    "fee_amount": fee_str, "net_amount": net_str,
                                }),
                            )
                            .execute(db)
                            .await
                            .map_err(|e| println!("[legacy_cron] TX debit insert: {}", e));

                            let _ = sqlx::query!(
                                r#"INSERT INTO transactions
                                    (sender_id, reciever_id, reference, type, amount, currency,
                                     narration, status, channel, meta)
                                   VALUES ($1, $2, $3, 'credit', $4, 'NGN', $5, $6, 'internal', $7)
                                   ON CONFLICT (reference) DO NOTHING"#,
                                plan.account_id,
                                recipient_account_id,
                                credit_ref,
                                net_bd,
                                narration,
                                tx_status,
                                serde_json::json!({
                                    "source": "legacy_plan", "plan_id": plan_id,
                                    "kin_id": kin.id, "share_percentage": kin.share_percentage,
                                }),
                            )
                            .execute(db)
                            .await
                            .map_err(|e| println!("[legacy_cron] TX credit insert: {}", e));

                            if tx_status == "failed" {
                                all_ok = false;
                            } else {
                                total_distributed += net_amount;

                                let recip_name = recip_firstname
                                    .clone()
                                    .unwrap_or_else(|| kin_display_name.clone());

                                // Push to internal recipient
                                if let Some(ref token) = recip_token {
                                    send_push(
                                        token,
                                        "💰 Legacy Plan Credit",
                                        &format!(
                                            "Dear {}, you have received ₦{} as part of a \
                                             legacy plan distribution from {}.",
                                            recip_name, net_str, owner_name
                                        ),
                                        Some(serde_json::json!({
                                            "type":      "legacy_plan_credit",
                                            "amount":    net_str,
                                            "plan_id":   plan_id,
                                            "reference": credit_ref,
                                        })),
                                    )
                                    .await;
                                }

                                // Email to internal recipient
                                let email = &recip_email;
                                let mailer = build_mailer(cfg);
                                if let Err(e) = mailer
                                    .send_legacy_executed_beneficiary(
                                        email,
                                        &recip_name,
                                        &owner_name,
                                        &net_str,
                                        kin.share_percentage,
                                        "Internal Transfer",
                                        &credit_ref,
                                        &execution_date,
                                    )
                                    .await
                                {
                                    println!(
                                        "[legacy_cron] Beneficiary email error for {}: {}",
                                        email, e
                                    );
                                }

                                owner_summary.push(LegacyBeneficiarySummary {
                                    name: recip_name.to_owned(),
                                    share_percentage: kin.share_percentage,
                                    net_amount: net_str.clone(),
                                    transfer_type: "Internal Transfer".to_string(),
                                });
                            }

                            println!(
                                "[legacy_cron] Internal — kin: {}, gross: ₦{}, fee: ₦{}, net: ₦{}, status: {}",
                                kin.id, amount_str, fee_str, net_str, tx_status
                            );
                        }
                        Err(e) => {
                            println!("[legacy_cron] PSB credit error for kin {}: {}", kin.id, e);
                            all_ok = false;
                        }
                    }
                }

                // ── External ─────────────────────────────────────────────────
                "external" => {
                    let bank_code = match &kin.bank_code {
                        Some(b) => b.clone(),
                        None => {
                            println!("[legacy_cron] Kin {} has no bank_code", kin.id);
                            all_ok = false;
                            continue;
                        }
                    };
                    let account_number = match &kin.account_number {
                        Some(n) => n.clone(),
                        None => {
                            println!("[legacy_cron] Kin {} has no account_number", kin.id);
                            all_ok = false;
                            continue;
                        }
                    };
                    let account_name = kin
                        .account_name
                        .clone()
                        .unwrap_or_else(|| kin.fullname.clone().unwrap_or_default());

                    // 15% platform fee
                    let fee_amount = share_amount * 0.15;
                    let net_amount = share_amount - fee_amount;
                    let fee_str = format!("{:.2}", fee_amount);
                    let net_str = format!("{:.2}", net_amount);
                    let net_bd = BigDecimal::from_str(&net_str).unwrap_or_default();

                    let psb_payload = serde_json::json!({
                        "customer": {
                            "account": {
                                "bank":                bank_code,
                                "name":                account_name,
                                "number":              account_number,
                                "senderaccountnumber": owner_account,
                                "sendername":          owner_name,
                            }
                        },
                        "narration": narration,
                        "order": {
                            "amount":      net_str,
                            "country":     "NGA",
                            "currency":    "NGN",
                            "description": narration,
                        },
                        "transaction": { "reference": reference },
                        "merchant": {
                            "isFee":              true,
                            "merchantFeeAccount": cfg._9psb_operational_account,
                            "merchantFeeAmount":  fee_str,
                        }
                    });

                    match PsbClient::new(cfg)
                        .post(
                            &mut redis_conn,
                            "/waas/api/v1/wallet_other_banks",
                            &psb_payload,
                        )
                        .await
                    {
                        Ok(res) => {
                            let tx_status = if res["status"].as_str().unwrap_or("").to_uppercase()
                                == "SUCCESS"
                            {
                                "success"
                            } else {
                                "failed"
                            };

                            let _ = sqlx::query!(
                                r#"INSERT INTO transactions
                                    (sender_id, reciever_id, reference, type, amount, currency,
                                     narration, status, channel,
                                     reciever_account_number, reciever_account_name, reciever_bank,
                                     meta)
                                   VALUES ($1, NULL, $2, 'debit', $3, 'NGN', $4, $5, 'external',
                                     $6, $7, $8, $9)
                                   ON CONFLICT (reference) DO NOTHING"#,
                                plan.account_id,
                                reference,
                                net_bd,
                                narration,
                                tx_status,
                                account_number,
                                account_name,
                                bank_code,
                                serde_json::json!({
                                    "source":            "legacy_plan",
                                    "plan_id":           plan_id,
                                    "kin_id":            kin.id,
                                    "share_percentage":  kin.share_percentage,
                                    "gross_amount":      amount_str,
                                    "fee_amount":        fee_str,
                                    "net_amount":        net_str,
                                    "psb_response_code": res["responseCode"].as_str().unwrap_or(""),
                                }),
                            )
                            .execute(db)
                            .await
                            .map_err(|e| println!("[legacy_cron] TX insert error: {}", e));

                            if tx_status == "failed" {
                                all_ok = false;
                            } else {
                                total_distributed += net_amount;

                                // Email only — external has no device token
                                let email = &kin.email;
                                let mailer = build_mailer(cfg);
                                if let Err(e) = mailer
                                    .send_legacy_executed_beneficiary(
                                        email,
                                        &kin_display_name,
                                        &owner_name,
                                        &net_str,
                                        kin.share_percentage,
                                        "Bank Transfer",
                                        &reference,
                                        &execution_date,
                                    )
                                    .await
                                {
                                    println!(
                                        "[legacy_cron] External beneficiary email error for {}: {}",
                                        email, e
                                    );
                                }

                                owner_summary.push(LegacyBeneficiarySummary {
                                    name: kin_display_name.clone(),
                                    share_percentage: kin.share_percentage,
                                    net_amount: net_str.clone(),
                                    transfer_type: "Bank Transfer".to_string(),
                                });
                            }

                            println!(
                                "[legacy_cron] External — kin: {}, bank: {}, gross: ₦{}, fee: ₦{}, net: ₦{}, status: {}",
                                kin.id, bank_code, amount_str, fee_str, net_str, tx_status
                            );
                        }
                        Err(e) => {
                            println!("[legacy_cron] PSB external error for kin {}: {}", kin.id, e);
                            all_ok = false;
                        }
                    }
                }

                _ => {
                    println!(
                        "[legacy_cron] Unknown account_type '{}' for kin {}",
                        kin.account_type, kin.id
                    );
                    all_ok = false;
                }
            }
        }

        // ── Mark plan result ──────────────────────────────────────────────────
        if all_ok {
            mark_executed(db, plan_id).await;

            // Push to owner
            if let Some(ref token) = plan.owner_device_token {
                send_push(
                    token,
                    "🏦 Legacy Plan Executed",
                    "Your legacy plan has been executed and your assets have been \
                     distributed to your named beneficiaries.",
                    Some(serde_json::json!({
                        "type":    "legacy_plan_executed",
                        "plan_id": plan_id,
                    })),
                )
                .await;
            }

            // Email to owner with full breakdown
            let email = &plan.owner_email;
            let mailer = build_mailer(cfg);
            let total_fmt = format!("{:.2}", total_distributed);
            let bene_count = owner_summary.len();
            if let Err(e) = mailer
                .send_legacy_executed_owner(
                    email,
                    &owner_name,
                    &total_fmt,
                    bene_count,
                    &owner_summary,
                    &execution_date,
                )
                .await
            {
                println!(
                    "[legacy_cron] Owner execution email error for {}: {}",
                    email, e
                );
            } else {
                println!("[legacy_cron] Owner execution email sent to {}", email);
            }
        } else {
            let _ = sqlx::query!(
                "UPDATE legacy_plans SET status = 'partial_failure', updated_at = NOW() WHERE id = $1",
                plan_id
            )
            .execute(db)
            .await
            .map_err(|e| println!("[legacy_cron] Failed to mark partial_failure for {}: {}", plan_id, e));

            println!(
                "[legacy_cron] Plan {} — completed with partial failures",
                plan_id
            );
        }
    }

    println!("[legacy_cron] ── Sweep complete ─────────────────────────────────");
}

async fn mark_executed(db: &PgPool, plan_id: uuid::Uuid) {
    let _ = sqlx::query!(
        r#"UPDATE legacy_plans
           SET executed_at = NOW(),
               status      = 'executed',
               updated_at  = NOW()
           WHERE id = $1"#,
        plan_id
    )
    .execute(db)
    .await
    .map_err(|e| println!("[legacy_cron] mark_executed error for {}: {}", plan_id, e));

    println!("[legacy_cron] Plan {} — marked as executed ✓", plan_id);
}

async fn send_push(token: &str, title: &str, body: &str, data: Option<serde_json::Value>) {
    crate::utils::fms::send_push_notification(token, title, body, data).await;
}

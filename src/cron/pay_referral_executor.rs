use crate::config::Config;
use crate::utils::fms::send_push_notification;
use crate::utils::psb::PsbClient;
use crate::utils::transaction_utils::generate_vas_reference;
use redis::aio::ConnectionManager;
use sqlx::PgPool;
use std::str::FromStr;

pub async fn run_referral_reward_payout(
    db: &PgPool,
    cfg: &Config,
    redis: &mut ConnectionManager,
) {
    let rows: Vec<_> = match sqlx::query!(
        r#"
        SELECT
            rr.id            AS reward_id,
            rr.referrer_id,
            rr.amount,
            a.account_number,
            a.account_name,
            a.device_token,
            a.firstname
        FROM referral_rewards rr
        JOIN accounts a ON a.id = rr.referrer_id
        WHERE rr.status    = 'pending'
          AND a.deleted_at IS NULL
        FOR UPDATE OF rr SKIP LOCKED
        "#
    )
    .fetch_all(db)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("[referral_payout] failed to fetch pending rewards: {}", e);
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    log::info!("[referral_payout] processing {} pending reward(s)", rows.len());

    for row in rows {
        let account_number = match row.account_number {
            Some(n) => n,
            None => {
                log::warn!(
                    "[referral_payout] referrer {} has no account number — skipping",
                    row.referrer_id
                );
                continue;
            }
        };

        let reference = generate_vas_reference();
        let amount_bd = bigdecimal::BigDecimal::from_str(&row.amount.to_string())
            .unwrap_or_default();

        // ── Call PSB credit transfer ──────────────────────────────────────────
        let psb_res = PsbClient::new(cfg)
            .post(
                redis,
                "/waas/api/v1/credit/transfer",
                &serde_json::json!({
                    "accountNo":     account_number,
                    "narration":     "Referral reward — 5 successful referrals",
                    "totalAmount":   row.amount,
                    "transactionId": reference,
                    "merchant": {
                        "isFee":              false,
                        "merchantFeeAccount": "",
                        "merchantFeeAmount":  ""
                    }
                }),
            )
            .await;

        let (psb_status, tx_status) = match psb_res {
            Ok(res) => {
                let s = res["status"].as_str().unwrap_or("").to_uppercase();
                let t = if s == "SUCCESS" { "paid" } else { "failed" };
                (s, t)
            }
            Err(e) => {
                log::error!(
                    "[referral_payout] PSB unreachable — reward_id={}: {}",
                    row.reward_id, e
                );
                (String::from("FAILED"), "failed")
            }
        };

        // ── Update reward status ──────────────────────────────────────────────
        if let Err(e) = sqlx::query!(
            r#"
            UPDATE referral_rewards
            SET status     = $1,
                updated_at = NOW()
            WHERE id = $2
            "#,
            tx_status,
            row.reward_id
        )
        .execute(db)
        .await
        {
            log::error!(
                "[referral_payout] failed to update reward status — reward_id={}: {}",
                row.reward_id, e
            );
        }

        // ── Insert transaction record ─────────────────────────────────────────
        if let Err(e) = sqlx::query!(
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
                NULL, $1, $2, 'credit', $3, 'NGN',
                'Referral reward — 5 successful referrals',
                $4, 'internal', $5, $6, '9PSB',
                $7
            )
            ON CONFLICT (reference) DO NOTHING
            "#,
            Some(row.referrer_id) as Option<uuid::Uuid>,
            reference,
            amount_bd,
            tx_status,
            account_number,
            row.account_name.clone().unwrap_or_default(),
            serde_json::json!({
                "reward_id":      row.reward_id,
                "referral_count": 5,
                "reward_type":    "referral",
            }),
        )
        .execute(db)
        .await
        {
            log::error!(
                "[referral_payout] transaction insert failed — reward_id={}: {}",
                row.reward_id, e
            );
        }

        log::info!(
            "[referral_payout] reward_id={} referrer={} status={} ref={}",
            row.reward_id, row.referrer_id, tx_status, reference
        );

        // ── Push notification ─────────────────────────────────────────────────
        if psb_status == "SUCCESS" {
            if let Some(token) = row.device_token.as_deref() {
                let firstname = row.firstname.unwrap_or_default();
                send_push_notification(
                    token,
                    "Referral Reward 🎉",
                    &format!(
                        "Congratulations {}! You've earned ₦1,000 for referring 5 friends. Keep referring to earn more!",
                        firstname
                    ),
                    Some(serde_json::json!({
                        "route":     "NotificationDetail",
                        "service":   "referral_reward",
                        "status":    "success",
                        "amount":    "1000",
                        "reference": reference,
                        "narration": "Referral reward — 5 successful referrals",
                    })),
                )
                .await;
            }
        }
    }

    log::info!("[referral_payout] ── cycle complete ──");
}
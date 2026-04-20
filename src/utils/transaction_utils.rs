use crate::utils::password_manager::verify_password;
use crate::utils::responder::{ApiResponse, ResponseStatus};
use actix_web::HttpResponse;
use sqlx::PgPool;

pub fn generate_reference() -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string(); // 14 chars
    let random: u32 = rand::random::<u32>() % 10000;
    format!("INT{}{:04}", timestamp, random) // 21 chars
}

pub fn generate_vas_reference() -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string(); // 14 chars
    let random: u32 = rand::random::<u32>() % 10000;
    format!("{}{:04}", timestamp, random) // 18 chars exactly
}

pub async fn verify_pin(
    pin:          &str,
    account_uuid: uuid::Uuid,
    db:           &PgPool,
    redis:        &mut redis::aio::ConnectionManager,
) -> Result<(), HttpResponse> {
    let cache_key = format!("pin_hash:{}", account_uuid);

    // ── Check Redis first ─────────────────────────────────────────────────────
    let pin_hash: String = match redis::cmd("GET")
        .arg(&cache_key)
        .query_async(redis)
        .await
        .unwrap_or(None)
    {
        Some(h) => h,
        None => {
            // ── Fetch from DB ─────────────────────────────────────────────────
            let row = sqlx::query!(
                "SELECT pin_hash FROM accounts WHERE id = $1 AND deleted_at IS NULL",
                account_uuid
            )
            .fetch_optional(db)
            .await
            .map_err(|e| {
                println!("[verify_pin] DB error: {}", e);
                HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Service temporarily unavailable".into(),
                    status:  ResponseStatus::ERROR,
                })
            })?;

            let hash: String = match row {
                Some(r) => match r.pin_hash {
                    Some(h) => h,
                    None => {
                        return Err(HttpResponse::BadRequest().json(ApiResponse {
                            message: "Payment PIN not set. Please set your PIN first.".into(),
                            status:  ResponseStatus::ERROR,
                        }));
                    }
                },
                None => {
                    return Err(HttpResponse::NotFound().json(ApiResponse {
                        message: "Account not found".into(),
                        status:  ResponseStatus::ERROR,
                    }));
                }
            };

            // ── Cache for 5 min ───────────────────────────────────────────────
            let _: Result<(), _> = redis::cmd("SETEX")
                .arg(&cache_key)
                .arg(300u64)
                .arg(&hash)
                .query_async(redis)
                .await;

            hash
        }
    };

    // ── Verify on blocking thread ─────────────────────────────────────────────
    let pin_input  = pin.to_owned();
    let pin_hash_c = pin_hash.clone();

    let is_valid = tokio::task::spawn_blocking(move || verify_password(&pin_input, &pin_hash_c))
        .await
        .unwrap_or(Ok(false));

    match is_valid {
        Ok(true) => Ok(()),
        _ => Err(HttpResponse::BadRequest().json(ApiResponse {
            message: "Incorrect payment PIN".into(),
            status:  ResponseStatus::ERROR,
        })),
    }
}



pub async fn verify_palm(_session_id: &str) -> Result<(), HttpResponse> {
    // ── Palm auth ─────────────────────────────────────────────────────────────────
    // TODO: call palm service to verify liveness session
    // e.g. palm_client.verify_session(_session_id).await
    Ok(())
}


pub fn get_grade(rate: f64) -> serde_json::Value {
    let (letter, gpa, description, status) = match rate as u32 {
        97..=100 => ("A+", "4.0", "Excellent",     "good"),
        93..=96  => ("A",  "4.0", "Excellent",     "good"),
        90..=92  => ("A-", "3.7", "Good",          "good"),
        87..=89  => ("B+", "3.3", "Above average", "average"),
        83..=86  => ("B",  "3.0", "Average",       "average"),
        80..=82  => ("B-", "2.7", "Below average", "average"),
        77..=79  => ("C+", "2.3", "Marginal",      "average"),
        73..=76  => ("C",  "2.0", "Marginal",      "average"),
        70..=72  => ("C-", "1.7", "Poor",          "bad"),
        _        => ("D",  "1.0", "Bad",           "bad"),
    };

    serde_json::json!({
        "letter": letter,
        "gpa": gpa,
        "description": description,
        "status": status
    })
}
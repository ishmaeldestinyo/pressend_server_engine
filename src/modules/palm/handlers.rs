use crate::config::Config;
use crate::modules::palm::schemas::{SetPalmCommitRequest, SetPalmStartRequest};
use crate::utils::jwt::AuthUser;
use crate::utils::responder::ValidationErrorResponse;
use crate::utils::responder::{ApiResponse, ResponseStatus};
use actix_web::{HttpResponse, Responder, web};
use sqlx::PgPool;
use validator::Validate;

async fn fetch_and_guard(db: &PgPool, account_uuid: uuid::Uuid) -> Result<(), HttpResponse> {
    let row = sqlx::query!(
        "SELECT status FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db)
    .await
    .map_err(|e| {
        println!("[set_palmprint] DB error: {}", e);
        HttpResponse::InternalServerError().json(ApiResponse {
            message: "Service temporarily unavailable".into(),
            status: ResponseStatus::ERROR,
        })
    })?;

    let status = match row {
        None => {
            return Err(HttpResponse::NotFound().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            }));
        }
        Some(r) => r.status,
    };

    match status.as_str() {
        "active" => Ok(()),
        "suspended" => Err(HttpResponse::Forbidden().json(ApiResponse {
            message: "Your account has been suspended. Please contact support.".into(),
            status: ResponseStatus::ERROR,
        })),
        "deleted" => Err(HttpResponse::Forbidden().json(ApiResponse {
            message: "This account no longer exists. Please contact support.".into(),
            status: ResponseStatus::ERROR,
        })),
        _ => Err(HttpResponse::Forbidden().json(ApiResponse {
            message: "Account is not active".into(),
            status: ResponseStatus::ERROR,
        })),
    }
}

// ── STEP 1 — POST /account/set-palmprint/start ───────────────────────────────
pub async fn set_palmprint_start(
    auth: AuthUser,
    body: web::Json<SetPalmStartRequest>,
    db: web::Data<PgPool>,
    http_client: web::Data<reqwest::Client>,
    cfg: web::Data<Config>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    if body.palm_type != "main" && body.palm_type != "panic" {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: "palm_type must be \"main\" or \"panic\"".into(),
            status: ResponseStatus::ERROR,
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

    // ── Auth + status guard ───────────────────────────────────────────────────
    if let Err(resp) = fetch_and_guard(&db, account_uuid).await {
        return resp;
    }

    // ── Panic palm requires main palm already enrolled ────────────────────────
    if body.palm_type == "panic" {
        let main_enrolled: Option<uuid::Uuid> = match sqlx::query_scalar!(
            "SELECT id FROM palm_enrollments
     WHERE account_id = $1 AND palm_type = 'main' AND is_active = TRUE",
            account_uuid
        )
        .fetch_optional(db.get_ref())
        .await
        {
            Ok(row) => row,
            Err(e) => {
                println!("[set_palmprint_start] DB error: {}", e);
                return HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Something went wrong".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        };
        if main_enrolled.is_none() {
            return HttpResponse::BadRequest().json(ApiResponse {
                message: "You must enroll your main palm before setting a panic palm.".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Forward to FastAPI ────────────────────────────────────────────────────
    let res = http_client
        .post(format!("{}/palm/enroll/start", cfg.palm_api_url))
        .json(&serde_json::json!({
            "account_id": auth.id,
            "palm_type":  body.palm_type,
        }))
        .send()
        .await;

    match res {
        Ok(r) => {
            let status_code = r.status();
            let data: serde_json::Value = r.json().await.unwrap_or_default();

            if !status_code.is_success() {
                println!("[set_palmprint_start] FastAPI error: {:?}", data);
                return HttpResponse::BadGateway().json(ApiResponse {
                    message: "Palm service returned an error. Please try again.".into(),
                    status: ResponseStatus::ERROR,
                });
            }

            let session_id = data["session_id"].as_str().unwrap_or_default();
            let palm_type = body.palm_type.as_str();

            // ── Challenges as comma-separated string for URL param ────────────
            let challenges_str = data["challenges"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();

            // ── ui_url — mobile opens this immediately in a WebView ───────────
            let ui_url = format!(
                "{}/ui?session_id={}&palm_type={}&challenges={}&mode=enroll",
                cfg.palm_api_url, session_id, palm_type, challenges_str,
            );

            HttpResponse::Ok().json(serde_json::json!({
                "status":        "ok",
                "session_id":    session_id,
                "websocket_url": data["websocket_url"],
                "challenges":    data["challenges"],
                "instructions":  data["instructions"],
                "expires_in":    data["expires_in"],
                "palm_type":     palm_type,
                "ui_url":        ui_url,
                "message":       "Open ui_url immediately to complete liveness, then call /set-palmprint/commit.",
            }))
        }
        Err(e) => {
            println!("[set_palmprint_start] Palm service unreachable: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

pub async fn set_palmprint_commit(
    auth: AuthUser,
    body: web::Json<SetPalmCommitRequest>,
    db: web::Data<PgPool>,
    http_client: web::Data<reqwest::Client>,
    cfg: web::Data<Config>,
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

    // ── Auth + status guard ───────────────────────────────────────────────────
    if let Err(resp) = fetch_and_guard(&db, account_uuid).await {
        return resp;
    }

    // ── Forward commit to FastAPI — saves vector to palm_enrollments ──────────
    let res = http_client
        .post(format!("{}/palm/enroll/commit", cfg.palm_api_url))
        .json(&serde_json::json!({
            "session_id": body.session_id,
        }))
        .send()
        .await;

    match res {
        Ok(r) => {
            let status_code = r.status();
            let data: serde_json::Value = r.json().await.unwrap_or_default();

            if !status_code.is_success() {
                let detail = data["detail"]
                    .as_str()
                    .unwrap_or("Enrollment could not be completed. Please try again.");
                return HttpResponse::BadRequest().json(ApiResponse {
                    message: detail.into(),
                    status: ResponseStatus::ERROR,
                });
            }

            let palm_type = data["palm_type"].as_str().unwrap_or("main");
            let action = data["action"].as_str().unwrap_or("created");

            HttpResponse::Ok().json(ApiResponse {
                message: format!(
                    "{} palm {} successfully. Your palm is now active for payments.",
                    capitalize(palm_type),
                    if action == "updated" {
                        "updated"
                    } else {
                        "enrolled"
                    },
                )
                .into(),
                status: ResponseStatus::SUCCESS,
            })
        }
        Err(e) => {
            println!("[set_palmprint_commit] Palm service unreachable: {}", e);
            HttpResponse::InternalServerError().json(ApiResponse {
                message: "Palm service temporarily unavailable.".into(),
                status: ResponseStatus::ERROR,
            })
        }
    }
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().to_string() + c.as_str(),
    }
}

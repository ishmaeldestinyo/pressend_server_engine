use crate::config::KafkaConfig;
use crate::kafka::KafkaProducer;
use crate::modules::legacy_plan::events::{
    LegacyPlanBeneficiaryAddedEvent, LegacyPlanBeneficiaryDeletedEvent, LegacyPlanKinNotifyEntry
};
use crate::modules::legacy_plan::schemas;
use crate::utils::{
    jwt::AuthUser,
    responder::{ApiResponse, ResponseStatus, ValidationErrorResponse},
};
use actix_web::HttpResponse;
use actix_web::{self, Responder, web};
use sqlx::PgPool;
use validator::Validate;


pub async fn delete_legacy_plan(
    auth: AuthUser,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
    kafka: web::Data<KafkaProducer>,
    kafka_cfg: web::Data<KafkaConfig>,
) -> impl Responder {
    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch plan ────────────────────────────────────────────────────────────
    let plan = sqlx::query!(
        r#"SELECT id, notify_beneficiaries
           FROM legacy_plans
           WHERE account_id = $1 AND deleted_at IS NULL"#,
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    let plan = match plan {
        Ok(Some(p)) => p,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "No active legacy plan found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[delete_legacy_plan] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Soft delete — set deleted_at and updated_at ───────────────────────────
    if let Err(e) = sqlx::query!(
        r#"UPDATE legacy_plans
           SET deleted_at = NOW(), updated_at = NOW()
           WHERE id = $1"#,
        plan.id
    )
    .execute(db.get_ref())
    .await
    {
        println!("[delete_legacy_plan] Update error: {}", e);
        return HttpResponse::InternalServerError().json(ApiResponse {
            message: "Something went wrong".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Invalidate cache ──────────────────────────────────────────────────────
    let cache_key = format!("legacy_plan:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();
    let _: Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await;

    // ── Fire and forget — notify beneficiaries if opted in ────────────────────
    if plan.notify_beneficiaries {
        let kin_rows = sqlx::query!(
            r#"SELECT fullname, email, share_percentage, legacy_message
               FROM legacy_next_of_kin
               WHERE legacy_id = $1"#,
            plan.id
        )
        .fetch_all(db.get_ref())
        .await;

        if let Ok(rows) = kin_rows {
            let event = LegacyPlanBeneficiaryDeletedEvent {
                account_id: auth.id.clone(),
                next_of_kin: rows
                    .iter()
                    .map(|k| LegacyPlanKinNotifyEntry {
                        email: k.email.trim().to_lowercase(),
                        fullname: k.fullname.clone().unwrap_or_default(),
                        share_percentage: k.share_percentage,
                        legacy_message: k.legacy_message.clone(),
                    })
                    .collect(),
            };

            kafka.publish(
                &kafka_cfg.kafka_topic_posthumous_plan_deleted,
                &auth.id,
                &event,
            );
        }
    }

    HttpResponse::NoContent().json(ApiResponse {
        message: "Legacy plan deleted successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}

pub async fn get_legacy_plan(
    auth: AuthUser,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    let account_uuid = match uuid::Uuid::parse_str(&auth.id) {
        Ok(id) => id,
        Err(_) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Invalid token".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Check Redis cache first ───────────────────────────────────────────────
    let cache_key = format!("legacy_plan:{}", auth.id);
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

    // ── Fetch plan ────────────────────────────────────────────────────────────
    let plan = sqlx::query!(
        r#"SELECT id, account_id, inactivity_days, grace_period_days, status,
                  notify_beneficiaries, last_activity_at, created_at, updated_at
           FROM legacy_plans
           WHERE account_id = $1 AND deleted_at IS NULL"#,
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    let plan = match plan {
        Ok(Some(p)) => p,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "No legacy plan found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[get_legacy_plan] DB error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Fetch next of kin ─────────────────────────────────────────────────────
    let kin_rows = sqlx::query!(
        r#"SELECT id, fullname, email, phone, account_type, share_percentage,
                  legacy_message, account_id, bank_code, bank_name,
                  account_number, account_name, created_at
           FROM legacy_next_of_kin
           WHERE legacy_id = $1"#,
        plan.id
    )
    .fetch_all(db.get_ref())
    .await;

    let next_of_kin = match kin_rows {
        Ok(rows) => rows
            .iter()
            .map(|k| {
                serde_json::json!({
                    "id": k.id,
                    "fullname": k.fullname,
                    "email": k.email,
                    "phone": k.phone,
                    "account_type": k.account_type,
                    "share_percentage": k.share_percentage,
                    "legacy_message": k.legacy_message,
                    "account_id": k.account_id,
                    "bank_code": k.bank_code,
                    "bank_name": k.bank_name,
                    "account_number": k.account_number,
                    "account_name": k.account_name,
                    "created_at": k.created_at,
                })
            })
            .collect::<Vec<_>>(),
        Err(e) => {
            println!("[get_legacy_plan] Kin fetch error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Build response ────────────────────────────────────────────────────────
    let data = serde_json::json!({
        "plan": {
            "id": plan.id,
            "account_id": plan.account_id,
            "inactivity_days": plan.inactivity_days,
            "grace_period_days": plan.grace_period_days,
            "status": plan.status,
            "notify_beneficiaries": plan.notify_beneficiaries,
            "last_activity_at": plan.last_activity_at,
            "created_at": plan.created_at,
            "updated_at": plan.updated_at,
        },
        "next_of_kin": next_of_kin,
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



pub async fn create_posthumous_plan(
    auth: AuthUser,
    body: web::Json<schemas::CreatePosthumousPlanRequest>,
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

    if let Err(e) = body.validate_shares() {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: e,
            status: ResponseStatus::ERROR,
        });
    }

    for kin in &body.next_of_kin {
        if let Err(e) = kin.validate_for_type() {
            return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: e,
                status: ResponseStatus::ERROR,
            });
        }
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

    // ── Fetch owner account — fail loudly if not found ────────────────────────
    let owner_email = match sqlx::query!(
        "SELECT email FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r.email.trim().to_lowercase(),
        Ok(None) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[create_legacy_plan] DB error fetching owner: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Ensure auth user is not listed as a beneficiary (by ID or email) ──────
    // This runs unconditionally for every kin entry regardless of account_type.
    // - account_id check: catches internal beneficiaries using the owner's UUID
    // - email check: catches any beneficiary (internal or external) using the
    //   owner's email, including cases where account_id is absent or omitted
    for kin in &body.next_of_kin {
        if let Some(kin_account_id) = kin.account_id {
            if kin_account_id == account_uuid {
                return HttpResponse::UnprocessableEntity().json(ApiResponse {
                    message: "You cannot add yourself as a beneficiary".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }

        if kin.email.trim().to_lowercase() == owner_email {
            return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: "You cannot add yourself as a beneficiary".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Check if plan already exists ──────────────────────────────────────────
    let existing = sqlx::query!(
        "SELECT id FROM legacy_plans WHERE account_id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    match existing {
        Ok(Some(_)) => {
            return HttpResponse::Conflict().json(ApiResponse {
                message: "A legacy plan/will already exists. Update or remove it.".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[create_legacy_plan] DB error checking existing plan: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
        _ => {}
    }

    // ── Validate internal account IDs exist ───────────────────────────────────
    for kin in &body.next_of_kin {
        if kin.account_type == schemas::NextOfKinAccountType::Internal {
            if let Some(kin_account_id) = kin.account_id {
                let exists = sqlx::query!(
                    "SELECT id, firstname FROM accounts WHERE id = $1 AND deleted_at IS NULL",
                    kin_account_id
                )
                .fetch_optional(db.get_ref())
                .await;

                match exists {
                    Ok(None) => {
                        let label = kin
                            .fullname
                            .clone()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| kin.email.trim().to_lowercase());

                        return HttpResponse::NotFound().json(ApiResponse {
                            message: format!("{} not found", label),
                            status: ResponseStatus::ERROR,
                        });
                    }
                    Err(e) => {
                        println!("[create_legacy_plan] DB error checking kin account: {}", e);
                        return HttpResponse::InternalServerError().json(ApiResponse {
                            message: "Something went wrong".into(),
                            status: ResponseStatus::ERROR,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    // ── Insert plan ───────────────────────────────────────────────────────────
    let grace_period = body.grace_period_days.unwrap_or(30);
    let notify_beneficiaries = body.notify_beneficiaries.unwrap_or(false);

    let plan = sqlx::query!(
        r#"INSERT INTO legacy_plans
            (account_id, inactivity_days, grace_period_days, status,
             notify_beneficiaries, last_activity_at, created_at, updated_at)
        VALUES ($1, $2, $3, 'active', $4, NOW(), NOW(), NOW())
        RETURNING id"#,
        account_uuid,
        body.inactivity_days,
        grace_period,
        notify_beneficiaries,
    )
    .fetch_one(db.get_ref())
    .await;

    let plan_id = match plan {
        Ok(r) => r.id,
        Err(e) => {
            println!("[create_legacy_plan] Insert error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Insert next of kin ────────────────────────────────────────────────────
    // account_id is only written for internal beneficiaries. External entries
    // always get NULL regardless of what the payload contains, so the owner's
    // UUID can never be stored against an external kin row by accident.
    for kin in &body.next_of_kin {
        let kin_account_id = if kin.account_type == schemas::NextOfKinAccountType::Internal {
            kin.account_id
        } else {
            None
        };

        let result = sqlx::query!(
            r#"INSERT INTO legacy_next_of_kin
               (legacy_id, fullname, email, phone, account_type, share_percentage,
                legacy_message, account_id, bank_code, bank_name, account_number, account_name)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
            plan_id,
            kin.fullname,
            kin.email.trim().to_lowercase(),
            kin.phone,
            format!("{:?}", kin.account_type).to_lowercase(),
            kin.share_percentage as f64,
            kin.legacy_message,
            kin_account_id,   // ← sanitized: None for external, validated UUID for internal
            kin.bank_code,
            kin.bank_name,
            kin.account_number,
            kin.account_name,
        )
        .execute(db.get_ref())
        .await;

        if let Err(e) = result {
            println!("[create_legacy_plan] Kin insert error: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Invalidate cache ──────────────────────────────────────────────────────
    let cache_key = format!("legacy_plan:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();
    let _: Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await;

    // ── Fire and forget — notify beneficiaries if opted in ────────────────────
    if notify_beneficiaries {
        let event = LegacyPlanBeneficiaryAddedEvent {
            account_id: auth.id.clone(),
            next_of_kin: body
                .next_of_kin
                .iter()
                .map(|kin| LegacyPlanKinNotifyEntry {
                    email: kin.email.trim().to_lowercase(),
                    fullname: kin.fullname.clone().unwrap_or_default(),
                    share_percentage: kin.share_percentage,
                    legacy_message: kin.legacy_message.clone(),
                })
                .collect(),
        };
        kafka.publish(
            &kafka_cfg.kafka_topic_legacy_beneficiary_added,
            &auth.id,
            &event,
        );
    }

    HttpResponse::Created().json(ApiResponse {
        message: "Legacy plan created successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}




pub async fn update_legacy_plan(
    auth: AuthUser,
    body: web::Json<schemas::UpdatePosthumousPlanRequest>,
    db: web::Data<PgPool>,
    redis: web::Data<redis::aio::ConnectionManager>,
) -> impl Responder {
    if let Err(errors) = body.0.validate() {
        return HttpResponse::UnprocessableEntity().json(ValidationErrorResponse {
            status: "error",
            message: "Invalid input",
            errors,
        });
    }

    if let Err(e) = body.validate_shares() {
        return HttpResponse::UnprocessableEntity().json(ApiResponse {
            message: e,
            status: ResponseStatus::ERROR,
        });
    }

    for kin in &body.next_of_kin {
        if let Err(e) = kin.validate_for_type() {
            return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: e,
                status: ResponseStatus::ERROR,
            });
        }
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

    // ── Fetch owner account — fail loudly if not found ────────────────────────
    let owner_email = match sqlx::query!(
        "SELECT email FROM accounts WHERE id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await
    {
        Ok(Some(r)) => r.email.trim().to_lowercase(),
        Ok(None) => {
            return HttpResponse::Unauthorized().json(ApiResponse {
                message: "Account not found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[update_legacy_plan] DB error fetching owner: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Ensure auth user is not listed as a beneficiary (by ID or email) ──────
    for kin in &body.next_of_kin {
        if let Some(kin_account_id) = kin.account_id {
            if kin_account_id == account_uuid {
                return HttpResponse::UnprocessableEntity().json(ApiResponse {
                    message: "You cannot add yourself as a beneficiary".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }

        if kin.email.trim().to_lowercase() == owner_email {
            return HttpResponse::UnprocessableEntity().json(ApiResponse {
                message: "You cannot add yourself as a beneficiary".into(),
                status: ResponseStatus::ERROR,
            });
        }
    }

    // ── Fetch existing plan ───────────────────────────────────────────────────
    let plan = sqlx::query!(
        "SELECT id FROM legacy_plans WHERE account_id = $1 AND deleted_at IS NULL",
        account_uuid
    )
    .fetch_optional(db.get_ref())
    .await;

    let plan_id = match plan {
        Ok(Some(p)) => p.id,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                message: "No active legacy plan found".into(),
                status: ResponseStatus::ERROR,
            });
        }
        Err(e) => {
            println!("[update_legacy_plan] DB error fetching plan: {}", e);
            return HttpResponse::InternalServerError().json(ApiResponse {
                message: "Something went wrong".into(),
                status: ResponseStatus::ERROR,
            });
        }
    };

    // ── Update plan fields ────────────────────────────────────────────────────
    if let Err(e) = sqlx::query!(
        r#"UPDATE legacy_plans
           SET inactivity_days   = COALESCE($1, inactivity_days),
               grace_period_days = COALESCE($2, grace_period_days),
               updated_at        = NOW()
           WHERE id = $3"#,
        body.inactivity_days,
        body.grace_period_days,
        plan_id,
    )
    .execute(db.get_ref())
    .await
    {
        println!("[update_legacy_plan] Plan update error: {}", e);
        return HttpResponse::InternalServerError().json(ApiResponse {
            message: "Something went wrong".into(),
            status: ResponseStatus::ERROR,
        });
    }

    // ── Validate internal account IDs exist ───────────────────────────────────
    for kin in &body.next_of_kin {
        if kin.account_type == schemas::NextOfKinAccountType::Internal {
            if let Some(kin_account_id) = kin.account_id {
                let exists = sqlx::query!(
                    "SELECT id FROM accounts WHERE id = $1 AND deleted_at IS NULL",
                    kin_account_id
                )
                .fetch_optional(db.get_ref())
                .await;

                match exists {
                    Ok(None) => {
                        let label = kin
                            .fullname
                            .clone()
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| kin.email.trim().to_lowercase());

                        return HttpResponse::NotFound().json(ApiResponse {
                            message: format!("{} not found", label),
                            status: ResponseStatus::ERROR,
                        });
                    }
                    Err(e) => {
                        println!("[update_legacy_plan] DB error checking kin account: {}", e);
                        return HttpResponse::InternalServerError().json(ApiResponse {
                            message: "Something went wrong".into(),
                            status: ResponseStatus::ERROR,
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    // ── Upsert each kin ───────────────────────────────────────────────────────
    for kin in &body.next_of_kin {
        // Sanitize: external beneficiaries must never carry an account_id
        let kin_account_id = if kin.account_type == schemas::NextOfKinAccountType::Internal {
            kin.account_id
        } else {
            None
        };

        // Internal → match by account_id
        // External → match by account_number
        let existing_kin_id: Option<uuid::Uuid> =
            if kin.account_type == schemas::NextOfKinAccountType::Internal {
                if let Some(kid) = kin_account_id {
                    sqlx::query!(
                        "SELECT id FROM legacy_next_of_kin WHERE legacy_id = $1 AND account_id = $2",
                        plan_id,
                        kid
                    )
                    .fetch_optional(db.get_ref())
                    .await
                    .ok()
                    .flatten()
                    .map(|r| r.id)
                } else {
                    None
                }
            } else {
                if let Some(ref acc_num) = kin.account_number {
                    sqlx::query!(
                        "SELECT id FROM legacy_next_of_kin WHERE legacy_id = $1 AND account_number = $2",
                        plan_id,
                        acc_num
                    )
                    .fetch_optional(db.get_ref())
                    .await
                    .ok()
                    .flatten()
                    .map(|r| r.id)
                } else {
                    None
                }
            };

        if let Some(kin_id) = existing_kin_id {
            // ── Update existing kin ───────────────────────────────────────────
            if let Err(e) = sqlx::query!(
                r#"UPDATE legacy_next_of_kin
                   SET fullname         = COALESCE($1, fullname),
                       email            = $2,
                       phone            = COALESCE($3, phone),
                       share_percentage = $4,
                       legacy_message   = $5,
                       bank_code        = COALESCE($6, bank_code),
                       bank_name        = COALESCE($7, bank_name),
                       account_number   = COALESCE($8, account_number),
                       account_name     = COALESCE($9, account_name)
                   WHERE id = $10"#,
                kin.fullname,
                kin.email.trim().to_lowercase(),
                kin.phone,
                kin.share_percentage as f64,
                kin.legacy_message,
                kin.bank_code,
                kin.bank_name,
                kin.account_number,
                kin.account_name,
                kin_id,
            )
            .execute(db.get_ref())
            .await
            {
                println!("[update_legacy_plan] Kin update error: {}", e);
                return HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Something went wrong".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        } else {
            // ── Insert new kin ────────────────────────────────────────────────
            if let Err(e) = sqlx::query!(
                r#"INSERT INTO legacy_next_of_kin
                   (legacy_id, fullname, email, phone, account_type, share_percentage,
                    legacy_message, account_id, bank_code, bank_name, account_number, account_name)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"#,
                plan_id,
                kin.fullname,
                kin.email.trim().to_lowercase(),
                kin.phone,
                format!("{:?}", kin.account_type).to_lowercase(),
                kin.share_percentage as f64,
                kin.legacy_message,
                kin_account_id,  // ← sanitized: None for external, validated UUID for internal
                kin.bank_code,
                kin.bank_name,
                kin.account_number,
                kin.account_name,
            )
            .execute(db.get_ref())
            .await
            {
                println!("[update_legacy_plan] Kin insert error: {}", e);
                return HttpResponse::InternalServerError().json(ApiResponse {
                    message: "Something went wrong".into(),
                    status: ResponseStatus::ERROR,
                });
            }
        }
    }

    // ── Invalidate cache ──────────────────────────────────────────────────────
    let cache_key = format!("legacy_plan:{}", auth.id);
    let mut redis_conn = redis.get_ref().clone();
    let _: Result<(), redis::RedisError> = redis::cmd("DEL")
        .arg(&cache_key)
        .query_async(&mut redis_conn)
        .await;

    HttpResponse::Ok().json(ApiResponse {
        message: "Legacy plan updated successfully".into(),
        status: ResponseStatus::SUCCESS,
    })
}


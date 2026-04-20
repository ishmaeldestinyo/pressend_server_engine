use yup_oauth2;

// ─────────────────────────────────────────────────────────────
// Google token response is no longer needed manually
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// GET ACCESS TOKEN (FIXED - Google recommended way)
// ─────────────────────────────────────────────────────────────
async fn get_fcm_access_token() -> Option<String> {
    let path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .unwrap_or_else(|_| "google-service.json".to_string());

    let key = match yup_oauth2::read_service_account_key(path).await {
        Ok(k) => k,
        Err(e) => {
            log::error!("[fcm] Failed to read service account file: {}", e);
            return None;
        }
    };

    let auth = match yup_oauth2::ServiceAccountAuthenticator::builder(key)
        .build()
        .await
    {
        Ok(a) => a,
        Err(e) => {
            log::error!("[fcm] Auth build error: {}", e);
            return None;
        }
    };

    let token = match auth
        .token(&["https://www.googleapis.com/auth/firebase.messaging"])
        .await
    {
        Ok(t) => t,
        Err(e) => {
            log::error!("[fcm] Token error: {}", e);
            return None;
        }
    };

    token.token().map(|s| s.to_string())
}

// ─────────────────────────────────────────────────────────────
// SEND PUSH NOTIFICATION
// ─────────────────────────────────────────────────────────────
pub async fn send_push_notification(
    device_token: &str,
    title: &str,
    body: &str,
    data: Option<serde_json::Value>,
) {
    let project_id = std::env::var("FCM_PROJECT_ID").unwrap_or_default();

    if project_id.is_empty() {
        log::error!("[fcm] Missing FCM_PROJECT_ID");
        return;
    }

    let access_token = match get_fcm_access_token().await {
        Some(t) => t,
        None => {
            log::error!("[fcm] Failed to get access token");
            return;
        }
    };

    let mut message = serde_json::json!({
        "message": {
            "token": device_token,
            "notification": {
                "title": title,
                "body": body
            },
            "android": {
                "priority": "high",
                "notification": {
                    "sound": "default",
                    "channel_id": "pressend_transfer"
                }
            },
            "apns": {
                "headers": {
                    "apns-priority": "10"
                },
                "payload": {
                    "aps": {
                        "sound": "default"
                    }
                }
            }
        }
    });

    if let Some(extra) = data {
        // FCM data field only accepts string values — convert everything
        if let Some(obj) = extra.as_object() {
            let stringified: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| {
                    let str_val = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), serde_json::Value::String(str_val))
                })
                .collect();
            message["message"]["data"] = serde_json::Value::Object(stringified);
        }
    }
    let url = format!(
        "https://fcm.googleapis.com/v1/projects/{}/messages:send",
        project_id
    );

    let client = reqwest::Client::new();

    let response = match client
        .post(&url)
        .bearer_auth(&access_token)
        .json(&message)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("[fcm] Request failed: {}", e);
            return;
        }
    };

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if status.is_success() {
        log::info!("[fcm] Push sent successfully");
    } else {
        log::warn!("[fcm] Push failed {}: {}", status, body);
    }
}

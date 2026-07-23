use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

// --- SQLx Database Enum ---
#[derive(Debug, Serialize, Deserialize, sqlx::Type, Clone, PartialEq, Eq)]
#[sqlx(type_name = "push_notification_status", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum PushNotificationStatus {
    Draft,
    Processing,
    Sent,
    Failed,
}

// --- Database Model ---
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct PushNotificationModel {
    pub id: Uuid,
    pub title: String,
    pub body: String,
    pub data: Option<serde_json::Value>,
    pub target_group: String,
    pub status: PushNotificationStatus,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,
    pub total_recipients: Option<i32>,
    pub successful_sends: Option<i32>,
    pub failed_sends: Option<i32>,
    pub error_log: Option<String>,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// --- Request DTOs ---

#[derive(Debug, Deserialize)]
pub struct SendPushNotificationSchema {
    pub title: String,
    pub body: String,
    pub data: Option<serde_json::Value>,
    #[serde(default = "default_target_group")]
    pub target_group: String,
}

fn default_target_group() -> String {
    "all".to_string()
}

#[derive(Debug, Deserialize)]
pub struct UpdatePushNotificationSchema {
    pub title: Option<String>,
    pub body: Option<String>,
    pub data: Option<serde_json::Value>,
    pub target_group: Option<String>,
    pub status: Option<PushNotificationStatus>,
}

#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    pub page: Option<i64>,
    pub limit: Option<i64>,
    pub status: Option<PushNotificationStatus>,
}

// --- Response Wrappers ---

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    pub data: Option<T>,
}

#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub limit: i64,
}
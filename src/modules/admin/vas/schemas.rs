use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct ListVasQuery {
    pub page: Option<i64>,
    pub limit: Option<i64>,
    pub vas_type: Option<String>,  // airtime, data, cable, power
    pub network: Option<String>,
    pub status: Option<String>,    // pending, success, failed
    pub sort_by: Option<String>,   // amount, created_at
    pub order: Option<String>,     // asc, desc
}
use serde::Deserialize;
use validator::Validate;
use crate::utils::password_manager::validate_password;

fn validate_role(role: &str) -> Result<(), validator::ValidationError> {
    match role {
        "admin" | "support" | "compliance" => Ok(()),
        _ => Err(validator::ValidationError::new("invalid_role")),
    }
}

#[derive(Debug, Deserialize, Validate)]
pub struct CreateStaffRequest {
    #[validate(email)]
    pub email: String,
    #[validate(custom(function = "validate_password"))]
    pub password: String,
    #[validate(length(min = 1))]
    pub firstname: String,
    #[validate(length(min = 1))]
    pub lastname: String,
    #[validate(custom(function = "validate_role"))]
    pub role: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ResetPasswordRequest {
    #[validate(custom(function = "validate_password"))]
    pub new_password: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct ListStaffQuery {
    pub page: Option<i64>,
    pub limit: Option<i64>,
    pub role: Option<String>,
    pub status: Option<String>,
}
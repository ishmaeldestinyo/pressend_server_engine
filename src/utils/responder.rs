use serde::{Serialize};

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum ResponseStatus {
    SUCCESS,
    ERROR,
}


#[derive(Serialize)]
pub struct ApiResponse {
    pub message: String,
    pub status: ResponseStatus,
}


#[derive(Serialize)]
pub struct AuthResponse {
    pub status: ResponseStatus,
    pub message: String,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>
}

#[derive(serde::Serialize)]
pub struct ValidationErrorResponse {
    pub status: &'static str,
    pub message: &'static str,
    pub errors: validator::ValidationErrors,
}

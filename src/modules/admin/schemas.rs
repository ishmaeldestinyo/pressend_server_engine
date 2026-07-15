use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct ListAccountsQuery {
    #[validate(range(min = 1))]
    pub page: Option<i64>,
    #[validate(range(min = 1, max = 100))]
    pub limit: Option<i64>,
    pub status: Option<String>,       // active, suspended, disabled
    pub account_type: Option<String>,
    pub current_tier: Option<i16>,
    pub q: Option<String>,            // free-text: name/email/phone/account_number
}

#[derive(Debug, Deserialize, Validate)]
pub struct AccountWalletQuery {
    // allow lookup by whichever identifier the admin has on hand
    pub account_number: Option<String>,
}


#[derive(Debug, Deserialize, Validate)]
pub struct ReviewAccountBody {
    pub status: String,
    pub note: Option<String>,
    pub reason: Option<String>,
}


#[derive(Debug, Deserialize, Validate)]
pub struct LoginRequest {
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 8, message = "Password must be at least 8 characters"))]
    pub password: String,
}
use crate::utils::password_manager::validate_password;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::OnceLock;
use validator::{Validate, ValidationError};

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
#[allow(dead_code)]
pub enum AccountType {
    REPRESENTATIVE,
    PERSONAL,
    BUSINESS,
}

impl fmt::Display for AccountType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccountType::REPRESENTATIVE => write!(f, "representative"),
            AccountType::PERSONAL => write!(f, "personal"),
            AccountType::BUSINESS => write!(f, "business"),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Gender {
    MALE,
    FEMALE,
}

#[allow(dead_code)]
impl Gender {
    pub fn to_9psb_value(&self) -> i32 {
        match self {
            Gender::MALE => 0,
            Gender::FEMALE => 1,
        }
    }
}

#[derive(Debug)]
#[derive(Deserialize, Serialize, Validate)]
pub struct SignupRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,

    #[validate(length(
        min = 8,
        message = "Device ID must be at least 8 characters long"
    ))]
    pub device_id: String,

    pub account_type: AccountType,

    #[validate(custom(function = "validate_password"))]
    pub password: String,

    pub reference: String, // dojah reference

    pub referral_code: Option<String>,

    // #[validate(length(min = 2, max = 10, message = "Invalid international country code"))]
    // pub intl_country_code: Option<String>, // e.g. "+233"

    // #[validate(length(min = 2, max = 100, message = "Invalid country name"))]
    // pub country_name: Option<String>, // e.g. "ghana"
}



#[derive(Deserialize, Serialize, Validate)]
pub struct SendOTPRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct VerifyOTPRequest {
    #[validate(email(message = "Invalid email address"))]
    pub email: String,

    #[validate(length(min = 5, max = 8, message = "Invalid OTP"))]
    pub otp: String,

    pub device_id: Option<String>,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct RefreshTokenRequest {
    #[validate(length(min = 1, message = "Refresh token is required"))]
    pub refresh_token: String,
}

#[derive(Serialize)]
pub struct Country {
    pub name: &'static str,
    pub short_name: &'static str,
    pub symbol: &'static str,
    pub currency_code: &'static str,
    pub country_code: &'static str,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct ChangePasswordRequest {
    #[validate(length(min = 1, message = "Current password is required"))]
    pub current_password: String,

    #[validate(custom(function = "validate_password"))]
    pub new_password: String,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct CloseAccountRequest {
    #[validate(length(min = 1, max = 200, message = "Please specify your reason"))]
    pub deletion_reason: Option<String>,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct SignInRequest {
    #[validate(email(message = "Invalid email address"))]
    pub email: String,

    #[validate(length(min = 1, message = "Password is required"))]
    pub password: String,

    #[validate(length(min = 1, message = "Device ID is required"))]
    pub device_id: String,
}

#[derive(Serialize, Deserialize, Validate)]
pub struct ResetPasswordVerification {
    #[validate(email(message = "Invalid email address"))]
    pub email: String,

    #[validate(length(min = 1, max = 7, message = "Invalid or expired OTP"))]
    pub otp: String,
}

#[derive(Deserialize, Serialize, Validate)]
pub struct ResetPasswordSubmit {
    #[validate(email(message = "Invalid email address"))]
    pub email: String,

    #[validate(custom(function = "validate_password"))]
    pub new_password: String,
}


#[derive(Deserialize, Serialize, Validate)]
pub struct UpgradeTier2Request {
    #[validate(length(min = 4, message = "Reference is required and must be at least 4 characters"))]
    pub reference: String,

    #[validate(length(min = 1, message = "House number is required"))]
    pub house_number: String,

    #[validate(length(min = 1, message = "Street name is required"))]
    pub street_name: String,

    #[validate(length(min = 1, message = "State is required"))]
    pub state: String,

    #[validate(length(min = 1, message = "City is required"))]
    pub city: String,

    #[validate(length(min = 1, message = "Local government is required"))]
    pub local_government: String,

    #[validate(length(min = 1, message = "Nearest landmark is required"))]
    pub nearest_landmark: String,

    #[validate(length(min = 2, max = 3, message = "PEP must be YES or NO"))]
    pub pep: String,

    #[validate(length(max = 5000000, message = "ID card front must not exceed 5000000 characters"))]
    pub id_card_front: String,

    #[validate(length(max = 5000000, message = "Signature is too large"))]
    pub customer_signature: String,

    #[validate(length(max = 5000000, message = "Utility bill is too large"))]
    pub utility_bill: String,
}


// ── Tier 3 — reuses ALL tier 2 fields from DB; only proof_of_address is new ───
#[derive(Deserialize, Serialize, Validate)]
pub struct UpgradeTier3Request {
    #[validate(length(min = 11, max = 11, message = "BVN must be 11 digits"))]
    pub bvn: Option<String>,

    #[validate(length(min = 11, max = 11, message = "NIN must be 11 digits"))]
    pub nin: Option<String>,

    #[validate(length(min = 1, max = 500000, message = "Proof of address is required for tier 3"))]
    pub proof_of_address: String,
}


#[derive(Debug, Deserialize, Validate)]
pub struct SearchAccountQuery {
    #[validate(length(min = 2, message = "Search query must be at least 2 characters"))]
    pub q: String,
}



// ── Payment PIN ───────────────────────────────────────────────────────────────
static DIGITS_ONLY_REGEX: OnceLock<Regex> = OnceLock::new();

fn digits_only_regex() -> &'static Regex {
    DIGITS_ONLY_REGEX.get_or_init(|| Regex::new(r"^\d+$").unwrap())
}

fn validate_pin_digits(pin: &str) -> Result<(), ValidationError> {
    if !digits_only_regex().is_match(pin) {
        let mut err = ValidationError::new("digits_only");
        err.message = Some("PIN must contain digits only".into());
        return Err(err);
    }
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct SetPaymentPinRequest {
    #[validate(length(min = 1, message = "Current password is required"))]
    pub current_password: String,

    #[validate(length(min = 4, max = 6, message = "PIN must be 4–6 digits"))]
    #[validate(custom(function = "validate_pin_digits"))]
    pub new_pin: String,
}


#[derive(Debug, serde::Deserialize, validator::Validate)]
pub struct UpdateDeviceTokenRequest {
    #[validate(length(min = 1, message = "Device token cannot be empty"))]
    pub device_token: String,
}


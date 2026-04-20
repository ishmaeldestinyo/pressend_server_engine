use crate::utils::password_manager::validate_password;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::OnceLock;
use validator::{Validate, ValidationError};

static NIN_USERID_REGEX: OnceLock<Regex> = OnceLock::new();

fn nin_userid_regex() -> &'static Regex {
    NIN_USERID_REGEX.get_or_init(|| Regex::new(r"^[A-Za-z]{6}-\d{4}$").unwrap())
}

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

// ── Tier 1 ────────────────────────────────────────────────────────────────────
#[derive(Deserialize, Serialize, Validate)]
pub struct OpenWalletRequest {
    #[validate(length(min = 1, max = 255, message = "First name is required"))]
    pub firstname: String,

    #[validate(length(min = 1, max = 255, message = "Last name is required"))]
    pub lastname: String,

    #[validate(length(min = 1, max = 255, message = "Other name is required"))]
    pub othername: Option<String>,

    #[validate(length(min = 11, max = 11, message = "Phone number must be 11 digits"))]
    pub phone_no: String,

    pub gender: Gender,

    // Format: dd/MM/yyyy
    #[validate(length(
        min = 10,
        max = 10,
        message = "Date of birth must be in dd/MM/yyyy format"
    ))]
    pub date_of_birth: String,

    #[validate(length(
        min = 1,
        max = 100,
        message = "Address is required and must not exceed 100 characters"
    ))]
    pub address: String,

    #[validate(length(min = 11, max = 11, message = "NIN must be 11 characters"))]
    pub nin: Option<String>,

    #[validate(regex(
        path = "nin_userid_regex()",
        message = "NIN User ID must be in format ABCDEF-0123"
    ))]
    pub nin_userid: Option<String>,

    #[validate(length(min = 11, max = 11, message = "BVN must be 11 characters"))]
    pub bvn: Option<String>,
}

impl OpenWalletRequest {
    pub fn validate_nin_or_bvn(&self) -> Result<(), ValidationError> {
        let has_bvn = self.bvn.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
        let has_nin = self.nin.as_ref().map(|s| !s.is_empty()).unwrap_or(false);

        if !has_bvn && !has_nin {
            let mut err = ValidationError::new("nin_or_bvn_required");
            err.message = Some("Either BVN or NIN must be provided".into());
            return Err(err);
        }

        Ok(())
    }
}

// ── Tier 2 — reuses bvn, nin, phone_no from DB; only asks for new fields ──────
#[derive(Deserialize, Serialize, Validate)]
pub struct UpgradeTier2Request {
    // ── Optional overrides from tier 1 (in case they were wrong/rejected) ─────
    #[validate(length(min = 11, max = 11, message = "BVN must be 11 digits"))]
    pub bvn: Option<String>,

    #[validate(length(min = 11, max = 11, message = "NIN must be 11 digits"))]
    pub nin: Option<String>,

    #[validate(length(min = 11, max = 11, message = "Phone number must be 11 digits"))]
    pub phone_no: Option<String>,

    // ── New: ID details ───────────────────────────────────────────────────────
    // 1=NationalID(NIN), 2=Driver's License, 3=Voter's Card, 4=International Passport
    #[validate(range(min = 1, max = 4, message = "ID type must be 1, 2, 3 or 4"))]
    pub id_type: i32,

    #[validate(length(min = 1, message = "ID number is required"))]
    pub id_number: String,

    // Format: yyyy-MM-dd
    #[validate(length(min = 10, max = 10, message = "ID issue date must be in yyyy-MM-dd format"))]
    pub id_issue_date: String,

    // Optional for NIN
    pub id_expiry_date: Option<String>,

    // ── New: Address breakdown ────────────────────────────────────────────────
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

    pub place_of_birth: Option<String>,

    // YES or NO
    #[validate(length(min = 2, max = 3, message = "PEP must be YES or NO"))]
    pub pep: String,

    // ── New: Base64 images ────────────────────────────────────────────────────
    #[validate(length(max = 5000000, message = "User photo is too large"))]
    pub user_photo: String,

    #[validate(length(max = 5000000, message = "ID card front must not exceed 100000 characters"))]
    pub id_card_front: String,

    pub id_card_back: Option<String>,

   #[validate(length(max = 5000000, message = "Signature is too large"))]
    pub customer_signature: String,

    #[validate(length(max = 5000000, message = "Utility bill is too large"))]
    pub utility_bill: String,

    // Optional for tier 2
    pub proof_of_address: Option<String>,
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

#[derive(Deserialize, Serialize, Validate)]
pub struct TogglePanicRequest {
    pub enabled: bool,
    pub message: Option<String>, // only needed when enabled = true
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
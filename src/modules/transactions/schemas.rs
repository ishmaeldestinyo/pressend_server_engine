use actix_web::HttpResponse;
use validator::Validate;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;

// ── Resolve / verify bank account ─────────────────────────────────────────────
#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct ResolveBankDetailRequest {
    #[validate(length(min = 10, max = 10, message = "Account number must be exactly 10 digits"))]
    pub account_number: String,

    #[validate(length(min = 3, max = 10, message = "Bank code is required"))]
    pub bank_code: String,
}

#[derive(Deserialize)]
pub struct PsbWalletTxQuery {
    pub from_date:        Option<String>,
    pub to_date:          Option<String>,
    pub number_of_items:  Option<u32>,
}


// ── Response shapes (shared) ──────────────────────────────────────────────────
#[derive(Debug, Serialize)]
pub struct BankResolveResponse {
    pub account_number: String,
    pub account_name: String,
    pub bank_code: String,
}


#[derive(Debug, Deserialize)]
pub struct TransactionFilterQuery {
    pub status: Option<String>,       // success, failed, reversed, pending
    pub from_interval: Option<String>, // "2024-01-01"
    pub to_interval: Option<String>,   // "2024-12-31"
}


#[derive(Debug, Deserialize, Validate)]
pub struct InternalTransferRequest {
    #[validate(length(min = 10, max = 10, message = "Invalid account number"))]
    pub recipient_account: String,   // 10-digit account_number

    #[validate(range(min = 100.0, message = "Minimum transfer amount is ₦100"))]
    pub amount: f64,

    #[validate(length(max = 1000))]
    pub narration: Option<String>,

    #[validate(length(min = 4, max = 6, message = "PIN must be 4-6 digits"))]
    pub pin: String,
}



#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum TransferAuth {
    /// User chose PIN verification
    Pin {
        #[serde(rename = "value")]
        pin: String,
    },
    /// User chose palm — liveness must already be completed on the palm service
    Palm {
        #[serde(rename = "value")]
        session_id: String,
    },
}



#[derive(Debug, Deserialize, Validate)]
pub struct ExternalTransferRequest {
    #[validate(length(min = 1, message = "Bank code is required"))]
    pub bank_code: String,

    #[validate(length(min = 1, message = "Recipient name is required"))]
    pub recipient_name: String,

    #[validate(length(equal = 10, message = "Recipient account number must be 10 digits"))]
    pub recipient_number: String,

    #[validate(range(min = 1.0, message = "Amount must be greater than 0"))]
    pub amount: f64,

    #[validate(length(max = 100))]
    pub narration: Option<String>,

    #[validate(length(min = 4, max=6, message = "PIN must be 4 digits"))]
    pub pin: String,
}


#[derive(Debug, Deserialize)]
pub struct PalmPaymentRequest {
    pub debit_account_number: String,
    pub frames: Vec<String>,
    pub front_camera: Option<bool>,
    pub amount: Decimal,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BankSuccessRateRequest {
    pub is_internal: bool,
    pub bank_code: Option<String>,
}

impl BankSuccessRateRequest {
    pub fn validate_fields(&self) -> Option<HttpResponse> {
        if !self.is_internal {
            match &self.bank_code {
                None => {
                    return Some(HttpResponse::UnprocessableEntity().json(serde_json::json!({
                        "status": "error",
                        "message": "bank_code is required for external transfers"
                    })));
                }
                Some(s) if s.trim().is_empty() => {
                    return Some(HttpResponse::UnprocessableEntity().json(serde_json::json!({
                        "status": "error",
                        "message": "bank_code is required for external transfers"
                    })));
                }
                _ => {}
            }
        }
        None
    }
}

#[derive(Debug, Deserialize, Serialize, Validate)]
pub struct FundWalletRequest {
    #[validate(length(min = 10, max = 10, message = "Account number must be exactly 10 digits"))]
    pub account_number: String,

    #[validate(range(min = 100.0, message = "Minimum funding amount is ₦100"))]
    pub amount: f64,

    #[validate(length(max = 100))]
    pub narration: Option<String>,
}
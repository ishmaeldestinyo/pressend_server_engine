use serde::{Deserialize, Serialize};
use validator::{Validate};

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum NextOfKinAccountType {
    Internal,
    External,
}

#[derive(Deserialize, Serialize, Validate, Clone)]
pub struct NextOfKinInput {
    pub id: Option<String>,
    pub fullname: Option<String>,

    #[validate(email(message = "Invalid email address"))]
    pub email: String,

    pub phone: Option<String>,

    pub account_type: NextOfKinAccountType,

    #[validate(range(
        min = 0.01,
        max = 100.0,
        message = "Share percentage must be between 0 and 100"
    ))]
    pub share_percentage: f64,
    pub  legacy_message: Option<String>,

    // ── Internal only ─────────────────────────────────────────────────────────
    pub account_id: Option<uuid::Uuid>,

    // ── External only ─────────────────────────────────────────────────────────
    pub bank_code: Option<String>,
    pub bank_name: Option<String>,
    pub account_number: Option<String>,
    pub account_name: Option<String>,
}

impl NextOfKinInput {
    pub fn validate_for_type(&self) -> Result<(), String> {
        match self.account_type {
            NextOfKinAccountType::Internal => {
                if self.account_id.is_none() {
                    return Err("account_id is required for internal next-of-kin".into());
                }
            }
            NextOfKinAccountType::External => {
                let missing: Vec<&str> = [
                    ("bank_code", self.bank_code.as_ref()),
                    ("bank_name", self.bank_name.as_ref()),
                    ("account_number", self.account_number.as_ref()),
                    ("account_name", self.account_name.as_ref()),
                ]
                .iter()
                .filter_map(|(name, val)| if val.is_none() { Some(*name) } else { None })
                .collect();

                if !missing.is_empty() {
                    return Err(format!(
                        "Missing fields for external next-of-kin: {}",
                        missing.join(", ")
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Validate)]
pub struct CreatePosthumousPlanRequest {
    #[validate(range(min = 1, message = "Inactivity days must be greater than 0"))]
    pub inactivity_days: i32,

    #[validate(range(min = 1, message = "Grace period days must be greater than 0"))]
    pub grace_period_days: Option<i32>,

    pub notify_beneficiaries: Option<bool>, // not stored — just triggers emails if true

    #[validate(length(min = 1, message = "At least one next-of-kin is required"))]
    pub next_of_kin: Vec<NextOfKinInput>,
}


impl CreatePosthumousPlanRequest {
    pub fn validate_shares(&self) -> Result<(), String> {
        let total: f64 = self.next_of_kin.iter().map(|k| k.share_percentage).sum();
        if (total - 100.0).abs() > 0.01 {
            return Err(format!(
                "Next-of-kin share percentages must sum to exactly 100. Got {:.2}.",
                total
            ));
        }
        Ok(())
    }
}

pub type UpdatePosthumousPlanRequest = CreatePosthumousPlanRequest;

#[derive(serde::Serialize, Clone)]
pub struct LegacyBeneficiarySummary {
    pub name:             String,
    pub share_percentage: f64,
    pub net_amount:       String,
    pub transfer_type:    String,
}

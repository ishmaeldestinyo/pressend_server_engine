use serde::{Deserialize};
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct AddBeneficiaryRequest {
    #[validate(length(min = 1, message = "Product type is required"))]
    pub product_type: String, // "bank" or "vas"

    #[validate(length(min = 1, message = "Recipient is required"))]
    pub recipient: String, // account number or phone number

    #[validate(length(min = 1, message = "Service name is required"))]
    pub service_name: String, // bank name or airtime/data/cable/power

    #[validate(length(min = 1, message = "Service code is required"))]
    pub service_code: String, // bank code or category_id

    pub label: Option<String>, // friendly name e.g "John Doe" or "Home DSTV"
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateBeneficiaryRequest {
    pub label:        Option<String>,
    pub service_name: Option<String>,
}
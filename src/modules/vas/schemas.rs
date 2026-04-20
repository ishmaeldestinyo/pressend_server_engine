
use serde::Deserialize;
use validator::Validate;


#[derive(Debug, Deserialize, Validate)]
pub struct AirtimePurchaseRequest {
    #[validate(length(equal = 11, message = "Phone number must be 11 digits"))]
    pub phone_number: String,

    #[validate(length(min = 1, message = "Network provider is required"))]
    pub network: String,

    #[validate(range(min = 50.0, message = "Minimum airtime purchase is ₦50"))]
    pub amount: f64,

    #[validate(length(min = 4, max=6, message = "PIN must be 4-6 digits"))]
    pub pin: String,
}

#[derive(Debug, Deserialize)]
pub struct VasTransactionFilterQuery {
    pub vas_type: Option<String>, // airtime, data, cable, power
    pub network:  Option<String>, // MTN, AIRTEL, GLO, 9MOBILE
}


#[derive(Debug, Deserialize, Validate)]
pub struct DataPurchaseRequest {
    #[validate(length(equal = 11, message = "Phone number must be 11 digits"))]
    pub phone_number: String,

    #[validate(length(min = 1, message = "Network provider is required"))]
    pub network: String,

    #[validate(length(min = 1, message = "Product ID is required"))]
    pub product_id: String,

    #[validate(length(min = 1, message = "Amount is required"))]
    pub amount: String,

    #[validate(length(min = 4, max = 6, message = "PIN must be 4-6 digits"))]
    pub pin: String,
}


#[derive(Debug, Deserialize, Validate)]
pub struct ValidateBillerRequest {
    #[validate(length(min = 1, message = "Biller ID is required"))]
    pub biller_id:   String,

    #[validate(length(min = 1, message = "Customer ID is required"))]
    pub customer_id: String,

    pub item_id:   Option<String>,
    pub amount:    Option<String>,
    pub firstname: Option<String>,
    pub lastname:  Option<String>,
}


#[derive(Debug, Deserialize, Validate)]
pub struct BillsPaymentRequest {
    #[validate(length(min = 1, message = "Customer ID is required"))]
    pub customer_id: String,

    #[validate(length(min = 1, message = "Biller ID is required"))]
    pub biller_id: String,

    // Optional — not all billers require a meter/plan type selection
    pub item_id: Option<String>,

    // Optional — not used by 9PSB bills API, frontend sends empty string
    pub customer_phone: Option<String>,

    #[validate(length(min = 1, message = "Customer name is required"))]
    pub customer_name: String,

    // Optional — not all billers return an otherField from validate
    pub other_field: Option<String>,

    #[validate(length(min = 1, message = "Amount is required"))]
    pub amount: String,

    #[validate(length(min = 4, max = 6, message = "PIN must be 4-6 digits"))]
    pub pin: String,
}
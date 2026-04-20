use serde::Deserialize;

#[derive(Deserialize)]
pub struct WebhookQuery {
    pub event: String,
}

#[derive(Deserialize)]
pub struct AccountUpgradeWebhookPayload {
    #[serde(rename = "transactionTrackingRef")]
    pub transaction_tracking_ref: String,  // this is the account_id we sent

    #[serde(rename = "accountNumber")]
    pub account_number: Option<String>,

    #[serde(rename = "accountName")]
    pub account_name: Option<String>,

    pub status: String,  // e.g. "SUCCESS" or "FAILED"

    pub message: Option<String>,
}
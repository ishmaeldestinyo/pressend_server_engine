#[derive(serde::Serialize, serde::Deserialize)]
pub struct AirtimePurchaseEvent {
    pub account_id:     String,
    pub account_number: String,
    pub phone_number:   String,
    pub network:        String,
    pub amount:         String,
    pub reference:      String,
}


#[derive(serde::Serialize, serde::Deserialize)]
pub struct BillsPaymentEvent {
    pub account_id:     String,
    pub account_number: String,
    pub customer_id:    String,
    pub biller_id:      String,
    pub item_id:        String,
    pub customer_phone: String,
    pub customer_name:  String,
    pub other_field:    String,
    pub amount:         String,
    pub reference:      String,
}


#[derive(serde::Serialize, serde::Deserialize)]
pub struct DataPurchaseEvent {
    pub account_id:     String,
    pub account_number: String,
    pub phone_number:   String,
    pub network:        String,
    pub product_id:     String,
    pub amount:         String,
    pub reference:      String,
}
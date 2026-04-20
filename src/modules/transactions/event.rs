use serde::{self};


#[derive(serde::Serialize, serde::Deserialize)]
pub struct InternalTransferInitiatedEvent {
    pub account_id:        String,
    pub email:             String,
    pub firstname:         String,
    pub sender_account:    String,
    pub recipient_account: String,
    pub reciever_id:       String, 
    pub amount:            String,
    pub narration:         String,
    pub reference:         String,
}



#[derive(serde::Serialize, serde::Deserialize)]
pub struct TransferReceivedEvent {
    pub recipient_account:  String,
    pub sender_account:     String,
    pub amount:             String,
    pub narration:          String,
    pub reference:          String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ExternalTransferInitiatedEvent {
    pub account_id:            String,
    pub email:                 String,
    pub firstname:             String,
    pub sender_account_number: String,
    pub sender_name:           String,
    pub bank_code:             String,
    pub recipient_name:        String,
    pub recipient_number:      String,
    pub amount:                String,
    pub narration:             String,
    pub reference:             String,
}
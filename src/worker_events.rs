use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct SignupEvent {
    pub email: String,
    pub password: String,
    pub device_id: String,
    pub account_type: String,
    pub address: String,
    pub otp_redis_key: String,
    // from Monnify NIN lookup
    pub nin: String,
    pub firstname: String,
    pub lastname: String,
    pub middlename: String,
    pub date_of_birth: String,
    pub gender: String,
    pub mobile_number: String,
}


#[derive(Serialize, Deserialize)]
pub struct VerifyEmailEvent {
    pub email: String,
}

#[derive(Serialize, Deserialize)]
pub struct SendOTPEvent {
    pub email: String,
    pub otp_redis_key: String,
}

#[derive(Serialize, Deserialize)]
pub struct ChangePasswordEvent {
    pub account_id: String,
    pub email: String,
    pub new_password: String,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteAccountEvent {
    pub account_id: String,
    pub email: String,
    pub firstname: Option<String>,
    pub deletion_reason: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct SuspiciousLoginEvent {
    pub account_id: String,
    pub email: String,
    pub firstname: String,
    pub current_trials: i64,
    pub previous_block: i64,
    pub ip: String,
}

#[derive(Serialize, Deserialize)]
pub struct NewDeviceLoginEvent {
    pub account_id: String,
    pub email: String,
    pub firstname: String,
    pub otp: String,
    pub otp_redis_key: String,
    pub ip: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateDeviceIdEvent {
    pub account_id: String,
    pub email: String,
    pub firstname: String,
    pub new_device_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct AccountLoggedInNotificationEvent {
    pub firstname: String,
    pub ip: String,
    pub email: String,

}

#[derive(Serialize, Deserialize)]
pub struct ChangeEmailEvent {
    pub account_id: String,
    pub old_email: String,
    pub new_email: String,
    pub firstname: String,
}


#[derive(Serialize, Deserialize)]
pub struct KycUpgradeStatusEvent {
    pub account_id: String,
    pub status: String,
    pub tier: i16,  
    pub account_number: Option<String>,
    pub account_name: Option<String>,
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Tier2UpgradeEvent {
    pub account_id: String,
    pub bvn: String,
    pub nin: String,
    pub phone_no: String,
    pub id_type: i32,
    pub id_number: String,
    pub id_issue_date: String,
    pub id_expiry_date: Option<String>,
    pub house_number: String,
    pub street_name: String,
    pub state: String,
    pub city: String,
    pub local_government: String,
    pub nearest_landmark: String,
    pub place_of_birth: Option<String>,
    pub pep: String,
    pub user_photo: String,
    pub id_card_front: String,
    pub id_card_back: Option<String>,
    pub customer_signature: String,
    pub utility_bill: String,
    pub proof_of_address: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Tier3UpgradeEvent {
    pub account_id: String,
    pub bvn: Option<String>,
    pub nin: Option<String>,
    pub proof_of_address: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct SetPaymentPinEvent {
    pub account_id: String,
    pub email:      String,
    pub firstname:  String,
}


#[derive(serde::Serialize, serde::Deserialize)]
pub struct InboundTransferEvent {
    pub session_id:        String,  // nipsessionid — idempotency key
    pub transaction_ref:   String,
    pub amount:            String,
    pub account_number:    String,  
    pub sender_name:       String,
    pub sender_account:    String,
    pub sender_bank:       String,
    pub narration:         String,
    pub status:         Option<String>,
}
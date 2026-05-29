#![allow(dead_code)]
use std::env; 
#[derive(Clone)] 
pub struct Config {
    pub app_name: String,
    pub app_base_url: String,
    pub frontend_url: String,

    pub db_host: String,
    pub db_port: u16,
    pub db_name: String,
    pub db_user: String,
    pub db_password: String,

    pub redis_url: String,
    pub jwt_secret: String,

    pub mail_user: String,
    pub mail_password: String,
    pub mail_server: String,
    pub mail_port: u16,

    pub psb_base_url: String,
    pub psb_client_id: String,
    pub psb_client_secret: String,
    pub psb_waas_username: String,
    pub psb_waas_password: String,
    pub psb_webhook_url: String,
    pub sqlx_offline: String,
    pub support_email: String,
    pub palm_api_url: String,

    pub _9psb_operational_account: String,

    pub vas_username: String,
    pub vas_password: String,

    pub psb_webhook_username: String,
    pub psb_webhook_password: String,


    // dojah credentials
    pub dojah_pk: String,
    pub dojah_sk: String,
    pub dojah_app_id: String,
    pub dojah_webhook_url: String,

}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv_override().ok();
        Self {
            palm_api_url: env::var("PALM_API_URL").expect("PALM_API_URL must be set"),

            app_name: env::var("APPLICATION_NAME").expect("APPLICATION_NAME must be set"),
            app_base_url: env::var("APPLICATION_BASEURL").expect("APPLICATION_BASEURL must be set"),
            frontend_url: env::var("FRONTEND_URL").expect("FRONTEND_URL must be set"),

            vas_username: env::var("_9PSB_VAT_USERNAME").expect("_9PSB_VAT_USERNAME must be set"),
            vas_password: env::var("_9PSB_VAT_PASSWORD").expect("_9PSB_VAT_PASSWORD must be set"),
           
            psb_webhook_username: env::var("_9PSB_WEBHOOK_USERNAME").expect("_9PSB_WEBHOOK_USERNAME must be set"),
            psb_webhook_password: env::var("_9PSB_WEBHOOK_PASSWORD").expect("_9PSB_WEBHOOK_PASSWORD must be set"),


            // dojah credentials
            dojah_pk: env::var("DOJAH_PK").expect("DOJAH_PK must be set"),
            dojah_sk: env::var("DOJAH_SK").expect("DOJAH_SK must be set"),
            dojah_app_id: env::var("DOJAH_APP_ID").expect("DOJAH_APP_ID must be set"),
            dojah_webhook_url: env::var("DOJAH_WEBHOOK").expect("DOJAH_WEBHOOK must be set"),
            

            db_host: env::var("POSTGRESQL_HOST").expect("POSTGRESQL_HOST must be set"),
            db_port: env::var("POSTGRESQL_PORT")
                .unwrap_or("5432".into())
                .parse()
                .unwrap(),
            db_name: env::var("POSTGRESQL_DBNAME").expect("POSTGRESQL_DBNAME must be set"),
            db_user: env::var("POSTGRESQL_USER").expect("POSTGRESQL_USER must be set"),
            db_password: env::var("POSTGRESQL_PASSWORD").expect("POSTGRESQL_PASSWORD must be set"),

            redis_url: env::var("REDIS_URL").expect("REDIS_URL must be set"),
            jwt_secret: env::var("JWT_SECRET_KEY").expect("JWT_SECRET_KEY must be set"),

            mail_user: env::var("MAIL_USER").expect("MAIL_USER must be set"),
            support_email: env::var("SUPPORT_EMAIL").expect("SUPPORT_EMAIL must be set"),
            mail_password: env::var("MAIL_PASSWORD").expect("MAIL_PASSWORD must be set"),
            mail_server: env::var("MAIL_SERVER").expect("MAIL_SERVER must be set"),
            mail_port: env::var("MAIL_PORT")
                .unwrap_or("465".into())
                .parse()
                .unwrap(),

            psb_base_url: env::var("_9PSB_BASEURL").expect("_9PSB_BASEURL must be set"),
            psb_client_id: env::var("_9PSB_CLIENT_ID").expect("_9PSB_CLIENT_ID must be set"),
            psb_client_secret: env::var("_9PSB_CLIENT_SECRET")
                .expect("_9PSB_CLIENT_SECRET must be set"),
            psb_waas_username: env::var("_9PSB_WAAS_USERNAME")
                .expect("_9PSB_WAAS_USERNAME must be set"),
            psb_waas_password: env::var("_9PSB_WAAS_PASSWORD")
                .expect("_9PSB_WAAS_PASSWORD must be set"),
            psb_webhook_url: env::var("_9PSB_WEBHOOK").expect("_9PSB_WEBHOOK must be set"),
            sqlx_offline: env::var("SQLX_OFFLINE").expect("SQLX_OFFLINE must be set"),

            _9psb_operational_account: env::var("_9PSB_OPERATIONAL_ACCOUNT")
                .expect("_9PSB_OPERATIONAL_ACCOUNT must be set"),
        }
    }

    pub fn database_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.db_user, self.db_password, self.db_host, self.db_port, self.db_name
        )
    }
}

pub struct KafkaConfig {
    pub kafka_broker: String,

    pub kafka_topic_external_transfer_initiated: String,

    // Auth topics
    pub kafka_topic_account_signup: String,
    pub kafka_topic_account_otp_send: String,
    pub kafka_topic_account_otp_verify: String,
    pub kafka_topic_account_signin: String,
    pub kafka_topic_account_suspicious_login: String,
    pub kafka_topic_account_delete: String,

    // Password topics
    pub kafka_topic_password_change_request: String,
    pub kafka_topic_password_change_verify: String,
    pub kafka_topic_password_change_submit: String,

    pub kafka_topic_account_new_device: String,

    // Email topics
    pub kafka_topic_email_change_request: String,
    pub kafka_topic_email_change_verify: String,
    pub kafka_topic_email_change_submit: String,

    // KYC Upgrade tier topics
    pub kafka_topic_open_wallet: String,
    pub kafka_topic_tier1_upgrade_request: String,
    pub kafka_topic_tier2_upgrade: String,
    pub kafka_topic_tier3_upgrade: String,
    pub kafka_topic_kyc_upgrade_status: String,

    pub kafka_topic_vas_airtime_requested: String,

    // Transfer topics
    pub kafka_topic_internal_transfer_initiated: String,
    pub kafka_topic_transfer_recieved: String,
    pub kafka_topic_transfer_inflow: String,

    // Panic topics
    pub kafka_topic_panic_activated: String,
    pub kafka_topic_panic_deactivated: String,

    // VAS topics
    pub kafka_topic_vas_requested: String,
    pub kafka_topic_vas_status: String,

    pub kafka_topic_vas_bills_requested: String,

    // Posthumous topics
    pub kafka_topic_posthumous_triggered: String,
    pub kafka_topic_posthumous_reminder: String,
    pub kafka_topic_posthumous_executing: String,
    pub kafka_topic_posthumous_beneficiary_notified: String,
    pub kafka_topic_posthumous_plan_deleted: String,

    pub kafka_topic_account_device_update: String,

    pub kafka_account_login_successful: String,
    pub kafka_topic_legacy_beneficiary_added: String,

    pub rust_log: String,

    pub kafka_topic_payment_pin_set: String,

    pub kafka_topic_vas_data_requested: String,
}

impl KafkaConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv_override().ok();
        Self {
            kafka_broker: env::var("KAFKA_BROKER").expect("KAFKA_BROKER must be set"),

            kafka_topic_vas_data_requested: env::var("KAFKA_TOPIC_VAS_DATA_REQUESTED")
                .expect("KAFKA_TOPIC_VAS_DATA_REQUESTED must be set"),

            kafka_topic_payment_pin_set: env::var("KAFKA_TOPIC_PAYMENT_PIN_SET")
                .expect("KAFKA_TOPIC_PAYMENT_PIN_SET must be set"),

            rust_log: env::var("RUST_LOG").expect("RUST_LOG must be set"),

            kafka_topic_external_transfer_initiated: env::var(
                "KAFKA_TOPIC_EXTERNAL_TRANSFER_INITIATED",
            )
            .expect("KAFKA_TOPIC_EXTERNAL_TRANSFER_INITIATED must be set"),

            kafka_topic_vas_bills_requested: env::var("KAFKA_TOPIC_VAS_BILLS_REQUESTED")
                .expect("KAFKA_TOPIC_VAS_BILLS_REQUESTED must be set"),

            // Auth topics
            kafka_topic_account_signup: env::var("KAFKA_TOPIC_ACCOUNT_SIGNUP")
                .expect("KAFKA_TOPIC_ACCOUNT_SIGNUP must be set"),
            kafka_topic_account_otp_send: env::var("KAFKA_TOPIC_ACCOUNT_OTP_SEND")
                .expect("KAFKA_TOPIC_ACCOUNT_OTP_SEND must be set"),
            kafka_topic_account_device_update: std::env::var("KAFKA_TOPIC_ACCOUNT_DEVICE_UPDATE")
                .unwrap(),
            kafka_topic_account_otp_verify: env::var("KAFKA_TOPIC_ACCOUNT_OTP_VERIFY")
                .expect("KAFKA_TOPIC_ACCOUNT_OTP_VERIFY must be set"),
            kafka_topic_account_signin: env::var("KAFKA_TOPIC_ACCOUNT_SIGNIN")
                .expect("KAFKA_TOPIC_ACCOUNT_SIGNIN must be set"),
            kafka_topic_account_new_device: std::env::var("KAFKA_TOPIC_ACCOUNT_NEW_DEVICE")
                .unwrap(),
            kafka_topic_account_suspicious_login: env::var("KAFKA_TOPIC_ACCOUNT_SUSPICIOUS_LOGIN")
                .expect("KAFKA_TOPIC_ACCOUNT_SUSPICIOUS_LOGIN must be set"),
            kafka_topic_account_delete: env::var("KAFKA_TOPIC_ACCOUNT_DELETE")
                .expect("KAFKA_TOPIC_ACCOUNT_DELETE must be set"),
            kafka_account_login_successful: env::var("KAFKA_ACCOUNT_LOGIN_SUCCESSFUL")
                .expect("KAFKA_ACCOUNT_LOGIN_SUCCESSFUL must be set"),

            // Password topics
            kafka_topic_password_change_request: env::var("KAFKA_TOPIC_PASSWORD_CHANGE_REQUEST")
                .expect("KAFKA_TOPIC_PASSWORD_CHANGE_REQUEST must be set"),
            kafka_topic_password_change_verify: env::var("KAFKA_TOPIC_PASSWORD_CHANGE_VERIFY")
                .expect("KAFKA_TOPIC_PASSWORD_CHANGE_VERIFY must be set"),
            kafka_topic_password_change_submit: env::var("KAFKA_TOPIC_PASSWORD_CHANGE_SUBMIT")
                .expect("KAFKA_TOPIC_PASSWORD_CHANGE_SUBMIT must be set"),
            kafka_topic_open_wallet: env::var("KAFKA_TOPIC_OPEN_WALLET")
                .expect("KAFKA_TOPIC_OPEN_WALLET must be set"),

            kafka_topic_vas_airtime_requested: env::var("KAFKA_TOPIC_VAS_AIRTIME_REQUESTED")
                .expect("KAFKA_TOPIC_VAS_AIRTIME_REQUESTED must be set"),

            // Email topics
            kafka_topic_email_change_request: env::var("KAFKA_TOPIC_EMAIL_CHANGE_REQUEST")
                .expect("KAFKA_TOPIC_EMAIL_CHANGE_REQUEST must be set"),
            kafka_topic_email_change_verify: env::var("KAFKA_TOPIC_EMAIL_CHANGE_VERIFY")
                .expect("KAFKA_TOPIC_EMAIL_CHANGE_VERIFY must be set"),
            kafka_topic_email_change_submit: env::var("KAFKA_TOPIC_EMAIL_CHANGE_SUBMIT")
                .expect("KAFKA_TOPIC_EMAIL_CHANGE_SUBMIT must be set"),

            // KYC Upgrade tier topics
            kafka_topic_tier1_upgrade_request: env::var("KAFKA_TOPIC_TIER1_UPGRADE_REQUEST")
                .expect("KAFKA_TOPIC_TIER1_UPGRADE_REQUEST must be set"),
            kafka_topic_tier2_upgrade: env::var("KAFKA_TOPIC_TIER2_UPGRADE")
                .expect("KAFKA_TOPIC_TIER2_UPGRADE must be set"),
            kafka_topic_tier3_upgrade: env::var("KAFKA_TOPIC_TIER3_UPGRADE")
                .expect("KAFKA_TOPIC_TIER3_UPGRADE must be set"),
            kafka_topic_kyc_upgrade_status: env::var("KAFKA_TOPIC_KYC_UPGRADE_STATUS")
                .expect("KAFKA_TOPIC_KYC_UPGRADE_STATUS must be set"),

            // Transfer topics
            kafka_topic_internal_transfer_initiated: env::var(
                "KAFKA_TOPIC_INTERNAL_TRANSFER_INITIATED",
            )
            .expect("KAFKA_TOPIC_INTERNAL_TRANSFER_INITIATED must be set"),
            kafka_topic_transfer_recieved: env::var("KAFKA_TOPIC_TRANSFER_RECIEVED")
                .expect("KAFKA_TOPIC_TRANSFER_RECIEVED must be set"),
            kafka_topic_transfer_inflow: env::var("KAFKA_TOPIC_TRANSFER_INFLOW")
                .expect("KAFKA_TOPIC_TRANSFER_INFLOW must be set"),

            // Panic topics
            kafka_topic_panic_activated: env::var("KAFKA_TOPIC_PANIC_ACTIVATED")
                .expect("KAFKA_TOPIC_PANIC_ACTIVATED must be set"),
            kafka_topic_panic_deactivated: env::var("KAFKA_TOPIC_PANIC_DEACTIVATED")
                .expect("KAFKA_TOPIC_PANIC_DEACTIVATED must be set"),

            // VAS topics
            kafka_topic_vas_requested: env::var("KAFKA_TOPIC_VAS_REQUESTED")
                .expect("KAFKA_TOPIC_VAS_REQUESTED must be set"),
            kafka_topic_vas_status: env::var("KAFKA_TOPIC_VAS_STATUS")
                .expect("KAFKA_TOPIC_VAS_STATUS must be set"),

            // Posthumous topics
            kafka_topic_posthumous_triggered: env::var("KAFKA_TOPIC_POSTHUMOUS_TRIGGERED")
                .expect("KAFKA_TOPIC_POSTHUMOUS_TRIGGERED must be set"),

            kafka_topic_legacy_beneficiary_added: env::var(
                "KAFKA_TOPIC_LEGACYPLAN_BENEFICIARY_ADDED",
            )
            .expect("KAFKA_TOPIC_LEGACYPLAN_BENEFICIARY_ADDED must be set"),

            kafka_topic_posthumous_reminder: env::var("KAFKA_TOPIC_POSTHUMOUS_REMINDER")
                .expect("KAFKA_TOPIC_POSTHUMOUS_REMINDER must be set"),

            kafka_topic_posthumous_executing: env::var("KAFKA_TOPIC_POSTHUMOUS_EXECUTING")
                .expect("KAFKA_TOPIC_POSTHUMOUS_EXECUTING must be set"),

            kafka_topic_posthumous_beneficiary_notified: env::var(
                "KAFKA_TOPIC_POSTHUMOUS_BENEFICIARY_NOTIFIED",
            )
            .expect("KAFKA_TOPIC_POSTHUMOUS_BENEFICIARY_NOTIFIED must be set"),

            kafka_topic_posthumous_plan_deleted: env::var(
                "KAFKA_TOPIC_POSTHUMOUS_BENEFICIARY_DELETED",
            )
            .expect("KAFKA_TOPIC_POSTHUMOUS_BENEFICIARY_DELETED must be set"),
        }
    }
}

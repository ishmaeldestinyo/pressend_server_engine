use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Config {
    pub app_id: String,
    pub secret_key: String,
    /// Production: "https://api.dojah.io"
    /// Sandbox:    "https://sandbox.dojah.io"
    pub base_url: String,
}

impl Config {
    pub fn new(app_id: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            secret_key: secret_key.into(),
            base_url: "https://api.dojah.io".into(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }
}

// ─── Error ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum DojahError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error [{status}]: {message}")]
    Api { status: u16, message: String },

    #[error("Parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    error: Option<String>,
    message: Option<String>,
}

impl ApiErrorBody {
    fn into_message(self) -> String {
        self.error.or(self.message).unwrap_or_else(|| "Unknown error".into())
    }
}

// ─── NIN Advanced models ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize)]
pub struct NinAdvanceEntity {
    pub nin: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub middle_name: Option<String>,
    pub date_of_birth: Option<String>,
    pub phone_number: Option<String>,
    pub photo: Option<String>,
    pub gender: Option<String>,
    pub employment_status: Option<String>,
    pub marital_status: Option<String>,
    pub birth_country: Option<String>,
    pub birth_lga: Option<String>,
    pub birth_state: Option<String>,
    pub educational_level: Option<String>,
    pub maiden_name: Option<String>,
    pub nspoken_lang: Option<String>,
    pub profession: Option<String>,
    pub religion: Option<String>,
    pub residence_address_line_1: Option<String>,
    pub residence_address_line_2: Option<String>,
    pub residence_status: Option<String>,
    pub residence_town: Option<String>,
    pub residence_lga: Option<String>,
    pub residence_state: Option<String>,
    pub ospoken_lang: Option<String>,
    pub origin_lga: Option<String>,
    pub origin_place: Option<String>,
    pub origin_state: Option<String>,
    pub height: Option<String>,
    pub p_first_name: Option<String>,
    pub p_middle_name: Option<String>,
    pub p_last_name: Option<String>,
    pub tax_id: Option<String>,
    pub tax_residency: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NinAdvanceResponse {
    pub entity: NinAdvanceEntity,
}

// ─── BVN + Selfie models ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct BvnSelfieRequest {
    pub bvn: String,
    /// Raw base64 buffer — strip the "data:image/jpeg;base64," prefix before passing in.
    pub selfie_image: String,
}

#[derive(Debug, Serialize)]
pub struct SelfieVerification {
    pub confidence_value: f64,
    pub match_: Option<bool>,
}

// serde sees "match" in JSON → our field match_
impl<'de> serde::de::Deserialize<'de> for SelfieVerification {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            confidence_value: f64,
            #[serde(rename = "match")]
            match_: Option<bool>,
        }
        let r = Raw::deserialize(d)?;
        Ok(Self { confidence_value: r.confidence_value, match_: r.match_ })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BvnSelfieEntity {
    pub bvn: Option<String>,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    pub date_of_birth: Option<String>,
    pub phone_number1: Option<String>,
    pub phone_number2: Option<String>,
    pub gender: Option<String>,
    /// Base64-encoded BVN photo
    pub image: Option<String>,
    pub selfie_verification: Option<SelfieVerification>,
    pub selfie_image_url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BvnSelfieResponse {
    pub entity: BvnSelfieEntity,
}

// ─── Client ──────────────────────────────────────────────────────────────────

pub struct DojahClient {
    pub base_url: String,
    pub cfg: Config,
    http: Client,
}

impl DojahClient {
    pub fn new(cfg: Config) -> Self {
        Self {
            base_url: cfg.base_url.clone(),
            cfg,
            http: Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn handle<T: for<'de> Deserialize<'de>>(
        resp: reqwest::Response,
    ) -> Result<T, DojahError> {
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if status.is_success() {
            serde_json::from_slice(&bytes).map_err(|e| DojahError::Parse(e.to_string()))
        } else {
            let body: ApiErrorBody = serde_json::from_slice(&bytes).unwrap_or(ApiErrorBody {
                error: Some(String::from_utf8_lossy(&bytes).into_owned()),
                message: None,
            });
            Err(DojahError::Api { status: status.as_u16(), message: body.into_message() })
        }
    }

    /// GET /api/v1/kyc/nin/advance — full NIN lookup with address, tax ID, next-of-kin, etc.
    pub async fn lookup_nin(&self, nin: &str) -> Result<NinAdvanceResponse, DojahError> {
        let resp = self
            .http
            .get(self.url("/api/v1/kyc/nin/advance"))
            .header("AppId", &self.cfg.app_id)
            .header("Authorization", &self.cfg.secret_key)
            .query(&[("nin", nin)])
            .send()
            .await?;
        Self::handle(resp).await
    }

    /// POST /api/v1/kyc/bvn/verify — verify BVN against a selfie image
    pub async fn verify_bvn_selfie(
        &self,
        req: BvnSelfieRequest,
    ) -> Result<BvnSelfieResponse, DojahError> {
        let resp = self
            .http
            .post(self.url("/api/v1/kyc/bvn/verify"))
            .header("AppId", &self.cfg.app_id)
            .header("Authorization", &self.cfg.secret_key)
            .json(&req)
            .send()
            .await?;
        Self::handle(resp).await
    }
}
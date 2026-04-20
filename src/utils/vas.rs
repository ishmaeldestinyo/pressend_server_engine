use crate::config::Config;
use redis::aio::ConnectionManager;

const VAS_ACCESS_TOKEN_KEY: &str = "9psb:vas:access_token";

pub struct VasClient {
    client:   reqwest::Client,
    base_url: String,
    username: String,
    password: String,
}

impl VasClient {
    pub fn new(cfg: &Config) -> Self {
        Self {
            client:   reqwest::Client::new(),
            base_url: cfg.psb_base_url.clone(), // same base URL as PSB
            username: cfg.vas_username.clone(),
            password: cfg.vas_password.clone(),
        }
    }

    // ── Authenticate ──────────────────────────────────────────────────────────
    async fn authenticate(&self) -> Result<serde_json::Value, String> {
        let res = self.client
            .post(format!("{}/identity/api/v1/authenticate", self.base_url))
            .json(&serde_json::json!({
                "username": self.username,
                "password": self.password,
            }))
            .send()
            .await
            .map_err(|e| format!("[vas] Auth HTTP error: {}", e))?;

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("[vas] Auth parse error: {}", e))
    }

    // ── Get access token (from cache or authenticate) ─────────────────────────
    pub async fn get_access_token(
        &self,
        redis: &mut ConnectionManager,
    ) -> Result<String, String> {
        // ── Check cache first ─────────────────────────────────────────────────
        let cached: Option<String> = redis::cmd("GET")
            .arg(VAS_ACCESS_TOKEN_KEY)
            .query_async(redis)
            .await
            .unwrap_or(None);

        if let Some(token) = cached {
            return Ok(token);
        }

        // ── Full authentication ───────────────────────────────────────────────
        let data = self.authenticate().await?;

        let token = data["data"]["accessToken"]
            .as_str()
            .ok_or("[vas] No accessToken in auth response")?
            .to_string();

        let expires_in = data["data"]["expiresIn"]
            .as_i64()
            .unwrap_or(3600) - 60;

        let _: Result<(), _> = redis::cmd("SETEX")
            .arg(VAS_ACCESS_TOKEN_KEY)
            .arg(expires_in)
            .arg(&token)
            .query_async::<_, ()>(redis)
            .await;

        println!("[vas] Authenticated — token cached for {}s", expires_in);

        Ok(token)
    }

    // ── POST ──────────────────────────────────────────────────────────────────
    pub async fn post(
        &self,
        redis:    &mut ConnectionManager,
        endpoint: &str,
        body:     &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let token = self.get_access_token(redis).await?;
        let url   = format!("{}{}", self.base_url, endpoint);

        let res = self.client
            .post(&url)
            .bearer_auth(&token)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("[vas] HTTP error: {}", e))?;

        if res.status() == 401 {
            println!("[vas] 401 — invalidating token and retrying");

            let _: Result<(), _> = redis::cmd("DEL")
                .arg(VAS_ACCESS_TOKEN_KEY)
                .query_async::<_, ()>(redis)
                .await;

            let token = self.get_access_token(redis).await?;

            let res = self.client
                .post(&url)
                .bearer_auth(&token)
                .json(body)
                .send()
                .await
                .map_err(|e| format!("[vas] HTTP retry error: {}", e))?;

            return res.json::<serde_json::Value>()
                .await
                .map_err(|e| format!("[vas] Response parse error: {}", e));
        }

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("[vas] Response parse error: {}", e))
    }

    // ── GET ───────────────────────────────────────────────────────────────────
    pub async fn get(
        &self,
        redis:    &mut ConnectionManager,
        endpoint: &str,
    ) -> Result<serde_json::Value, String> {
        let token = self.get_access_token(redis).await?;
        let url   = format!("{}{}", self.base_url, endpoint);

        let res = self.client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("[vas] HTTP error: {}", e))?;

        if res.status() == 401 {
            println!("[vas] 401 — invalidating token and retrying");

            let _: Result<(), _> = redis::cmd("DEL")
                .arg(VAS_ACCESS_TOKEN_KEY)
                .query_async::<_, ()>(redis)
                .await;

            let token = self.get_access_token(redis).await?;

            let res = self.client
                .get(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| format!("[vas] HTTP retry error: {}", e))?;

            return res.json::<serde_json::Value>()
                .await
                .map_err(|e| format!("[vas] Response parse error: {}", e));
        }

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("[vas] Response parse error: {}", e))
    }
}
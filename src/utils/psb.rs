use crate::config::Config;
use redis::aio::ConnectionManager;

const WAAS_ACCESS_TOKEN_KEY: &str = "9psb:waas:access_token";
const WAAS_REFRESH_TOKEN_KEY: &str = "9psb:waas:refresh_token";

pub struct PsbClient {
    client: reqwest::Client,
    base_url: String,
    client_id: String,
    client_secret: String,
    username: String,
    password: String,
}

impl PsbClient {
    pub fn new(cfg: &Config) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: cfg.psb_base_url.clone(),
            client_id: cfg.psb_client_id.clone(),
            client_secret: cfg.psb_client_secret.clone(),
            username: cfg.psb_waas_username.clone(),
            password: cfg.psb_waas_password.clone(),
        }
    }

    // ── Authenticate ──────────────────────────────────────────────────────────
    async fn authenticate(&self) -> Result<serde_json::Value, String> {

        let res = self.client
            .post(format!("{}/waas/api/v1/authenticate", self.base_url))
            .json(&serde_json::json!({
                "username": self.username,
                "password": self.password,
                "clientId": self.client_id,
                "clientSecret": self.client_secret,
            }))
            .send()
            .await
            .map_err(|e| format!("Auth HTTP error: {}", e))?;

        let body = res.text().await.map_err(|e| format!("Auth read error: {}", e))?;

        serde_json::from_str(&body).map_err(|e| format!("Auth parse error: {}", e))
    }

    // ── Refresh token ─────────────────────────────────────────────────────────
    async fn refresh(&self, refresh_token: &str) -> Result<serde_json::Value, String> {
        let res = self.client
            .post(format!("{}/waas/api/v1/token/refresh", self.base_url))
            .json(&serde_json::json!({
                "refreshToken": refresh_token,
                "clientId": self.client_id,
                "clientSecret": self.client_secret,
            }))
            .send()
            .await
            .map_err(|e| format!("Refresh HTTP error: {}", e))?;

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Refresh parse error: {}", e))
    }

    // ── Store tokens in Redis ─────────────────────────────────────────────────
    async fn store_tokens(
        &self,
        redis: &mut ConnectionManager,
        data: &serde_json::Value,
    ) -> Result<(), String> {
        let access_token = data["accessToken"].as_str()
            .ok_or("No accessToken in response")?;
        let refresh_token = data["refreshToken"].as_str()
            .ok_or("No refreshToken in response")?;
        let access_expires_in = data["expiresIn"].as_i64().unwrap_or(3600) - 60;
        let refresh_expires_in = data["refreshExpiresIn"].as_i64().unwrap_or(86400) - 60;

        redis::cmd("SETEX")
            .arg(WAAS_ACCESS_TOKEN_KEY)
            .arg(access_expires_in)
            .arg(access_token)
            .query_async::<_, ()>(redis)
            .await
            .map_err(|e| format!("Redis set access token error: {}", e))?;

        redis::cmd("SETEX")
            .arg(WAAS_REFRESH_TOKEN_KEY)
            .arg(refresh_expires_in)
            .arg(refresh_token)
            .query_async::<_, ()>(redis)
            .await
            .map_err(|e| format!("Redis set refresh token error: {}", e))?;

        Ok(())
    }

    // ── Get access token (from cache or authenticate) ─────────────────────────
    pub async fn get_access_token(&self, redis: &mut ConnectionManager) -> Result<String, String> {
        // ── Check cached access token ─────────────────────────────────────────
        let access_token: Option<String> = redis::cmd("GET")
            .arg(WAAS_ACCESS_TOKEN_KEY)
            .query_async(redis)
            .await
            .unwrap_or(None);

        if let Some(token) = access_token {
            return Ok(token);
        }

        // ── Try refresh token ─────────────────────────────────────────────────
        let refresh_token: Option<String> = redis::cmd("GET")
            .arg(WAAS_REFRESH_TOKEN_KEY)
            .query_async(redis)
            .await
            .unwrap_or(None);

        if let Some(refresh) = refresh_token {
            match self.refresh(&refresh).await {
                Ok(data) => {
                    if let Some(token) = data["accessToken"].as_str() {
                        let token = token.to_string();
                        let _ = self.store_tokens(redis, &data).await;
                        return Ok(token);
                    }
                }
                Err(e) => println!("[psb] Refresh failed: {} — re-authenticating", e),
            }
        }


        // ── Full authentication ───────────────────────────────────────────────
        let data = self.authenticate().await?;

        let token = data["accessToken"]
            .as_str()
            .ok_or("No accessToken in auth response")?
            .to_string();

        let _ = self.store_tokens(redis, &data).await;
        Ok(token)
    }

    // ── Make authenticated request ────────────────────────────────────────────
    pub async fn post(
        &self,
        redis: &mut ConnectionManager,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let token = self.get_access_token(redis).await?;
        let url = format!("{}{}", self.base_url, endpoint);

        let res = self.client
            .post(&url)
            .bearer_auth(&token)
            .json(body)
            .send()
            .await
            .map_err(|e| format!("HTTP error: {}", e))?;

        // ── If 401 — invalidate token and retry once ──────────────────────────
        if res.status() == 401 {
            let _ = redis::cmd("DEL")
                .arg(WAAS_ACCESS_TOKEN_KEY)
                .query_async::<_, ()>(redis)
                .await;

            let token = self.get_access_token(redis).await?;
            let res = self.client
                .post(&url)
                .bearer_auth(&token)
                .json(body)
                .send()
                .await
                .map_err(|e| format!("HTTP retry error: {}", e))?;

            return res.json::<serde_json::Value>()
                .await
                .map_err(|e| format!("Response parse error: {}", e));
        }

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Response parse error: {}", e))
    }

    pub async fn get(
        &self,
        redis: &mut ConnectionManager,
        endpoint: &str,
    ) -> Result<serde_json::Value, String> {
        let token = self.get_access_token(redis).await?;
        let url = format!("{}{}", self.base_url, endpoint);

        let res = self.client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| format!("HTTP error: {}", e))?;

        res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Response parse error: {}", e))
    }
}
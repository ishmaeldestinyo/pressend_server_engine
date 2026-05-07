use crate::config::Config;
use redis::aio::ConnectionManager;

const MONNIFY_ACCESS_TOKEN_KEY: &str = "9psb:waas:access_token";

#[derive(Clone)]
pub struct MonnifyClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    secret_key: String,
    test_mode: bool,
}

impl MonnifyClient {
    pub fn new(cfg: &Config) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: "https://api.monnify.com/api/v1".to_string(),
            api_key: cfg.monnify_apikey.clone(),
            secret_key: cfg.monnify_secret_key.clone(),
            test_mode: cfg.monnify_test_mode == "true",
        }
    }

    async fn authenticate(&self) -> Result<String, String> {
        let credentials = format!("{}:{}", self.api_key, self.secret_key);

        use base64::{Engine as _, engine::general_purpose};
        let encoded_credentials = general_purpose::STANDARD.encode(&credentials);

        let res = self.client
            .post(format!("{}/auth/login", self.base_url))
            .header("Authorization", format!("Basic {}", encoded_credentials))
            .send()
            .await
            .map_err(|e| format!("Auth HTTP error: {}", e))?;

        let json: serde_json::Value = res.json().await
            .map_err(|e| format!("Auth parse error: {}", e))?;

        json.get("responseBody")
            .and_then(|b| b.get("accessToken"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| "Missing accessToken in auth response".to_string())
    }

    async fn store_access_token(
        &self,
        conn: &mut ConnectionManager,
        token: &str,
    ) -> Result<(), String> {
        redis::cmd("SET")
            .arg(MONNIFY_ACCESS_TOKEN_KEY)
            .arg(token)
            .arg("EX")
            .arg(3600)
            .query_async(conn)
            .await
            .map_err(|e| format!("Redis error: {}", e))
    }

    async fn get_access_token(
        &self,
        conn: &mut ConnectionManager,
    ) -> Result<String, String> {
        redis::cmd("GET")
            .arg(MONNIFY_ACCESS_TOKEN_KEY)
            .query_async::<_, Option<String>>(conn)
            .await
            .map_err(|e| format!("Redis error: {}", e))?
            .ok_or_else(|| "Access token not found in Redis".to_string())
    }

    async fn get_or_refresh_token(
        &self,
        conn: &mut ConnectionManager,
    ) -> Result<String, String> {
        match self.get_access_token(conn).await {
            Ok(token) => Ok(token),
            Err(_) => {
                let new_token = self.authenticate().await?;
                self.store_access_token(conn, &new_token).await?;
                Ok(new_token)
            }
        }
    }

    async fn post(
        &self,
        conn: &mut ConnectionManager,
        endpoint: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let token = self.get_or_refresh_token(conn).await?;

        let res = self.client
            .post(format!("{}/{}", self.base_url, endpoint))
            .header("Authorization", format!("Bearer {}", token))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("HTTP error: {}", e))?;

        let json = res.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Parse error: {}", e))?;

        // if token is invalid, clear it, re-authenticate and retry once
        if json.get("error").and_then(|v| v.as_str()) == Some("invalid_token") {
            println!("[monnify] Token invalid — clearing and retrying");

            let new_token = self.authenticate().await?;
            self.store_access_token(conn, &new_token).await?;

            let res = self.client
                .post(format!("{}/{}", self.base_url, endpoint))
                .header("Authorization", format!("Bearer {}", new_token))
                .json(&body)
                .send()
                .await
                .map_err(|e| format!("HTTP error on retry: {}", e))?;

            return res.json::<serde_json::Value>()
                .await
                .map_err(|e| format!("Parse error on retry: {}", e));
        }

        Ok(json)
    }

    pub async fn verify_nin(
        &self,
        conn: &mut ConnectionManager,
        nin: &str,
    ) -> Result<NinDetails, String> {
        if self.test_mode {
            println!("[monnify] Test mode enabled — returning mock NIN data");
            return Ok(Self::mock_nin_details(nin));
        }

        let response = self
            .post(conn, "vas/nin-details", serde_json::json!({ "nin": nin }))
            .await?;

        println!("[monnify] NIN response: {}", serde_json::to_string_pretty(&response).unwrap_or_default());

        let successful = response
            .get("requestSuccessful")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !successful {
            let msg = response
                .get("responseMessage")
                .and_then(|v| v.as_str())
                .unwrap_or("NIN verification failed");
            return Err(msg.to_string());
        }

        let body = response
            .get("responseBody")
            .ok_or("Missing responseBody")?;

        Ok(NinDetails {
            nin: body["nin"].as_str().unwrap_or("").to_string(),
            last_name: body["lastName"].as_str().unwrap_or("").to_string(),
            first_name: body["firstName"].as_str().unwrap_or("").to_string(),
            middle_name: body["middleName"].as_str().unwrap_or("").to_string(),
            date_of_birth: body["dateOfBirth"].as_str().unwrap_or("").to_string(),
            gender: body["gender"].as_str().unwrap_or("").to_string(),
            mobile_number: body["mobileNumber"].as_str().unwrap_or("").to_string(),
        })
    }

    fn mock_nin_details(nin: &str) -> NinDetails {
        // generate unique data per request using nin as seed
        let last_names = ["WILES", "JOHNSON", "SMITH", "ADAMS", "IBRAHIM", "OKAFOR", "BELLO", "JAMES"];
        let first_names = ["BENJAMIN", "SAMUEL", "GRACE", "DAVID", "FATIMA", "CHIDI", "AMARA", "JOHN"];
        let middle_names = ["CHUKS", "EMEKA", "TUNDE", "FAITH", "BLESSING", "KEMI", "PETER", "PAUL"];
        let genders = ["MALE", "FEMALE"];

        // use last digit of nin to pick from lists
        let idx = nin.chars().last()
            .and_then(|c| c.to_digit(10))
            .unwrap_or(0) as usize;

        let last_name = last_names[idx % last_names.len()];
        let first_name = first_names[(idx + 2) % first_names.len()];
        let middle_name = middle_names[(idx + 4) % middle_names.len()];
        let gender = genders[idx % genders.len()];

        // generate a mobile number using nin digits
        let mobile = format!("234811{}", &nin[nin.len().saturating_sub(7)..]);

        // generate a date of birth using nin digits
        let year = 1980 + (idx as u32 * 3 % 30);
        let month = (idx % 12) + 1;
        let day = (idx % 28) + 1;
        let dob = format!("{}-{:02}-{:02}", year, month, day);

        println!(
            "[monnify] Mock NIN — nin: {}, name: {} {} {}, dob: {}, gender: {}, mobile: {}",
            nin, first_name, middle_name, last_name, dob, gender, mobile
        );

        NinDetails {
            nin: nin.to_string(),
            last_name: last_name.to_string(),
            first_name: first_name.to_string(),
            middle_name: middle_name.to_string(),
            date_of_birth: dob,
            gender: gender.to_string(),
            mobile_number: mobile,
        }
    }
}

#[derive(Debug)]
pub struct NinDetails {
    pub nin: String,
    pub last_name: String,
    pub first_name: String,
    pub middle_name: String,
    pub date_of_birth: String,
    pub gender: String,
    pub mobile_number: String,
}
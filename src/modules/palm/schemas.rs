use serde::Deserialize;
use validator::Validate;



#[derive(Debug, Deserialize, Validate)]
pub struct SetPalmStartRequest {
    #[validate(length(min = 4, max = 5, message = "palm_type must be \"main\" or \"panic\""))]
    pub palm_type: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct SetPalmCommitRequest {
    #[validate(length(min = 1, message = "session_id is required"))]
    pub session_id: String,
}


use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct LegacyPlanKinNotifyEntry {
    pub email: String,
    pub fullname: String,
    pub share_percentage: f64,
    pub legacy_message: Option<String>, 
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LegacyPlanBeneficiaryAddedEvent {
    pub account_id: String,
    pub next_of_kin: Vec<LegacyPlanKinNotifyEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LegacyPlanBeneficiaryDeletedEvent {
    pub account_id: String,
    pub next_of_kin: Vec<LegacyPlanKinNotifyEntry>,
}
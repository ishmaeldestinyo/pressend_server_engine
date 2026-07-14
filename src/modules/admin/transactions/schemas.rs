use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct ListTransactionsQuery {
    pub page: Option<i64>,
    pub limit: Option<i64>,
    pub channel: Option<String>,   // internal, external, vas
    pub status: Option<String>,    // pending, success, failed, reversed
    pub sort_by: Option<String>,   // amount, created_at
    pub order: Option<String>,     // asc, desc
    pub q: Option<String>,         // reference search
}

#[derive(Debug, Deserialize, Validate)]
pub struct TopTransactionsQuery {
    pub limit: Option<i64>,
    pub channel: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct SearchBetweenAccountsQuery {
    #[validate(length(min = 1))]
    pub account_a: String,
    #[validate(length(min = 1))]
    pub account_b: String,
    pub date: Option<String>,               // YYYY-MM-DD
    pub max_amount: Option<bigdecimal::BigDecimal>,
    pub status: Option<String>,
}
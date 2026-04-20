use sqlx::{postgres::{PgPoolOptions, PgConnectOptions}, PgPool};
use std::time::Duration;
use std::str::FromStr;

pub async fn create_pool(database_url: &str) -> PgPool {
    let connect_options = PgConnectOptions::from_str(database_url)
        .expect("❌ Invalid database URL")
        .statement_cache_capacity(0) // pgbouncer compatibility — disable prepared statements
        .options([("tcp_keepalives_idle", "60")]);

    PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(1800))
        .connect_with(connect_options)
        .await
        .expect("❌ Failed to connect to PostgreSQL via PgBouncer")
}


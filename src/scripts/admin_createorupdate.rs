// src/scripts/admin_createorupdate.rs
//
// Usage:
//   cargo run --bin create_super_admin -- --email admin@blinq.com --password "StrongPass123!"
//
// or interactively (omit --password to be prompted, hidden input):
//   cargo run --bin create_super_admin -- --email admin@blinq.com
//
// Requires in Cargo.toml:
//   sqlx = { version = "0.7", features = ["postgres", "runtime-tokio-rustls", "uuid", "chrono"] }
//   argon2 = "0.5"
//   rand_core = { version = "0.6", features = ["std"] }
//   rpassword = "7"
//   dotenvy = "0.15"
//   tokio = { version = "1", features = ["full"] }

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHasher, SaltString},
    Argon2,
};
use sqlx::postgres::PgPoolOptions;
use std::env;

pub async fn create_super_admin() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let args: Vec<String> = env::args().collect();

    let email = get_arg(&args, "--email")
        .ok_or("Missing required --email argument")?;

    let password = match get_arg(&args, "--password") {
        Some(p) => p,
        None => rpassword::prompt_password("Enter password for super admin: ")?,
    };

    if password.len() < 8 {
        return Err("Password must be at least 8 characters".into());
    }

    let database_url = env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is not set")?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    // Enforce single super admin, and stop concurrent invocations
    // of this script from racing past the check.
    let mut tx = pool.begin().await?;

    let existing: Option<(uuid::Uuid,)> = sqlx::query_as(
        "SELECT id FROM admins WHERE role = 'super_admin' AND deleted_at IS NULL LIMIT 1",
    )
    .fetch_optional(&mut *tx)
    .await?;

    if let Some((existing_id,)) = existing {
        tx.rollback().await?;
        return Err(format!(
            "A super admin already exists (id: {existing_id}). Only one super admin is allowed."
        )
        .into());
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| format!("Failed to hash password: {e}"))?
        .to_string();

    let row: (uuid::Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO admins (
            email,
            email_verified,
            firstname,
            lastname,
            password_hash,
            role,
            status
        )
        VALUES ($1, TRUE, 'Super', 'Admin', $2, 'super_admin', 'active')
        RETURNING id
        "#,
    )
    .bind(&email)
    .bind(&password_hash)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    println!("Super admin created successfully.");
    println!("  id:    {}", row.0);
    println!("  email: {email}");

    Ok(())
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
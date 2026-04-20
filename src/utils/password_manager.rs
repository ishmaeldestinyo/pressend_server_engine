
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2, Params, Version, Algorithm,
};


pub fn validate_password(password: &str) -> Result<(), validator::ValidationError> {
    let has_uppercase = password.chars().any(|c| c.is_uppercase());
    let has_lowercase = password.chars().any(|c| c.is_lowercase());
    let has_digit = password.chars().any(|c| c.is_numeric());
    let has_special = password.chars().any(|c| "!@#$%^&*()_+-=[]{}|;:,.<>?".contains(c));
    let has_min_length = password.len() >= 8;

    if !has_uppercase || !has_lowercase || !has_digit || !has_special || !has_min_length {
        return Err(validator::ValidationError::new(
            "Password must contain at least 1 uppercase, 1 lowercase, 1 number and 1 special character"
        ));
    }

    Ok(())
}


pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    
    // tune: reduce memory & iterations for speed vs security tradeoff
    let params = Params::new(
        16 * 1024, // 16MB memory (default is 64MB)
        2,         // 2 iterations (default is 3)
        1,         // 1 thread
        None,
    ).unwrap();

    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("Hash error: {}", e))
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    let parsed_hash = PasswordHash::new(hash)
        .map_err(|e| format!("Hash parse error: {}", e))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}
-- Add migration script here
ALTER TABLE kyc_verifications ALTER COLUMN account_id DROP NOT NULL;
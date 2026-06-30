-- Add migration script here
ALTER TABLE accounts 
    ADD COLUMN IF NOT EXISTS kyc_reference VARCHAR(100) 
-- Add migration script here
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS intl_area_code VARCHAR(10) NOT NULL DEFAULT '+234', -- include the + sign
    ADD COLUMN IF NOT EXISTS country_name VARCHAR(50) NOT NULL DEFAULT 'nigeria' -- always in lowercase
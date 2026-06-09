-- Add migration script here
CREATE TABLE IF NOT EXISTS kyc_bvn_data (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kyc_verification_id UUID NOT NULL UNIQUE REFERENCES kyc_verifications(id) ON DELETE CASCADE,
    bvn                 TEXT NOT NULL,
    first_name          TEXT,
    middle_name         TEXT,
    last_name           TEXT,
    gender              TEXT,
    date_of_birth       DATE,
    phone_number        TEXT,
    image_url           TEXT,
    app_id              TEXT,
    customer_ref        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kyc_bvn_data_bvn          ON kyc_bvn_data(bvn);
CREATE INDEX idx_kyc_bvn_data_phone_number ON kyc_bvn_data(phone_number);
CREATE INDEX idx_kyc_bvn_data_last_name    ON kyc_bvn_data(last_name);
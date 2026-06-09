-- Add migration script here
-- ─────────────────────────────────────────────
-- KYC: main verification record (one per webhook)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kyc_verifications (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id          UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

    -- Dojah identifiers
    reference_id        TEXT        NOT NULL UNIQUE,  -- e.g. "pressend_b5de689f-..."
    widget_id           TEXT,                         -- "6a1d8da9076cb35d0f77e7b4"

    -- Verification classification
    id_type             TEXT        NOT NULL,          -- "NIN", "BVN", "PASSPORT", …
    verification_type   TEXT        NOT NULL,          -- "nin", "bvn", …
    verification_mode   TEXT        NOT NULL,          -- "LIVENESS", "SELFIE", …
    verification_status TEXT        NOT NULL,          -- "Completed", "Pending", "Failed"
    verification_url    TEXT,
    value               TEXT,                          -- raw ID number submitted (e.g. NIN)

    -- Overall outcome
    status              BOOLEAN     NOT NULL DEFAULT FALSE,
    aml_status          BOOLEAN     NOT NULL DEFAULT FALSE,
    message             TEXT,

    -- Selfie / liveness
    selfie_url          TEXT,
    liveness_score      NUMERIC(8, 5),                -- 97.88285
    match_score         NUMERIC(8, 5),                -- 99.0

    -- Device / network metadata
    device_info         TEXT,                         -- raw device string
    ip_info             JSONB,                        -- full ipinfo object

    -- Full raw payload for auditability / schema changes
    raw_payload         JSONB       NOT NULL,

    verified_at         TIMESTAMPTZ,                  -- null until status = true
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kyc_verifications_account_id         ON kyc_verifications(account_id);
CREATE INDEX idx_kyc_verifications_status             ON kyc_verifications(status);
CREATE INDEX idx_kyc_verifications_verification_type  ON kyc_verifications(verification_type);
CREATE INDEX idx_kyc_verifications_id_type            ON kyc_verifications(id_type);
CREATE INDEX idx_kyc_verifications_value              ON kyc_verifications(value);  -- lookup by NIN/BVN
CREATE INDEX idx_kyc_verifications_ip_info            ON kyc_verifications USING GIN(ip_info);
CREATE INDEX idx_kyc_verifications_raw_payload        ON kyc_verifications USING GIN(raw_payload);


-- ─────────────────────────────────────────────
-- KYC: parsed NIN entity data (1-to-1 with kyc_verifications when id_type = 'NIN')
-- Kept separate so other ID types (BVN, passport) get their own table later
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS kyc_nin_data (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    kyc_verification_id     UUID        NOT NULL UNIQUE REFERENCES kyc_verifications(id) ON DELETE CASCADE,

    -- Core identity fields from government_data.nin.entity
    nin                     TEXT        NOT NULL,
    first_name              TEXT,
    middle_name             TEXT,
    last_name               TEXT,
    gender                  TEXT,
    date_of_birth           DATE,
    phone_number            TEXT,
    image_url               TEXT,               -- Dojah-hosted NIN photo

    -- Dojah internal refs
    app_id                  TEXT,               -- "64edd87d7c96780040e2b91a"
    customer_ref            TEXT,               -- Dojah customer UUID

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kyc_nin_data_nin             ON kyc_nin_data(nin);
CREATE INDEX idx_kyc_nin_data_phone_number    ON kyc_nin_data(phone_number);
CREATE INDEX idx_kyc_nin_data_last_name       ON kyc_nin_data(last_name);


-- ─────────────────────────────────────────────
-- Trigger: keep updated_at fresh on kyc_verifications
-- ─────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

CREATE TRIGGER kyc_verifications_updated_at
    BEFORE UPDATE ON kyc_verifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();






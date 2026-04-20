CREATE TABLE IF NOT EXISTS beneficiaries (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

    product_type    TEXT        NOT NULL CHECK (product_type IN ('bank', 'vas')),

    -- for bank: account number | for vas: phone number, meter number etc
    recipient       TEXT        NOT NULL,

    -- for bank: bank name | for vas: airtime, data, cable, power
    service_name    TEXT        NOT NULL,

    -- for bank: bank code | for vas: category_id
    service_code    TEXT        NOT NULL,

    -- friendly label e.g "John Doe" or "Home DSTV"
    label           TEXT,

    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_beneficiaries_account_id   ON beneficiaries(account_id);
CREATE INDEX idx_beneficiaries_product_type ON beneficiaries(product_type);
CREATE INDEX idx_beneficiaries_deleted_at   ON beneficiaries(deleted_at);

-- prevent duplicate active beneficiaries for same account + recipient + service
CREATE UNIQUE INDEX idx_beneficiaries_unique_active
    ON beneficiaries(account_id, recipient, service_code)
    WHERE deleted_at IS NULL;-- Add migration script here

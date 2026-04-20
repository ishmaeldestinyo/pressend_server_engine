-- Add migration script here
CREATE TABLE IF NOT EXISTS legacy_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

    meta JSONB,

    inactivity_days INT NOT NULL,
    grace_period_days INT NOT NULL DEFAULT 30,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    notify_beneficiaries BOOLEAN NOT NULL DEFAULT FALSE,

    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    triggered_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    executed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legacy_next_of_kin (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    legacy_id UUID NOT NULL REFERENCES legacy_plans(id) ON DELETE CASCADE,

    fullname VARCHAR(255),
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20),

    account_type VARCHAR(50) NOT NULL,
    share_percentage FLOAT8 NOT NULL,

    legacy_message TEXT,

    -- ── Internal ──────────────────────────────────────────────────────────────
    account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,

    -- ── External ──────────────────────────────────────────────────────────────
    bank_code VARCHAR(20),
    bank_name VARCHAR(255),
    account_number VARCHAR(20),
    account_name VARCHAR(255),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Partial unique index: one active plan per account (allows re-creation after soft delete)
CREATE INDEX IF NOT EXISTS legacy_plans_account_id_active_idx
    ON legacy_plans (account_id)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_legacy_plans_account_id ON legacy_plans(account_id);
CREATE INDEX IF NOT EXISTS idx_legacy_plans_status ON legacy_plans(status);
CREATE INDEX IF NOT EXISTS idx_legacy_next_of_kin_legacy_id ON legacy_next_of_kin(legacy_id);
CREATE INDEX IF NOT EXISTS idx_legacy_next_of_kin_account_id ON legacy_next_of_kin(account_id);

CREATE INDEX IF NOT EXISTS idx_legacy_plans_cron
    ON legacy_plans (status, executed_at, deleted_at, last_activity_at)
    WHERE deleted_at IS NULL AND executed_at IS NULL AND status = 'active';

-- Add migration script here
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS referral_code VARCHAR(20) UNIQUE,
    ADD COLUMN IF NOT EXISTS referred_by   UUID REFERENCES accounts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_accounts_referral_code ON accounts(referral_code);
CREATE INDEX IF NOT EXISTS idx_accounts_referred_by   ON accounts(referred_by);
-- Add migration script here
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_phone_number_key;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_nin_key;

-- Replace with partial unique indexes on active accounts only
CREATE UNIQUE INDEX idx_accounts_phone_active ON accounts(phone_number) WHERE status = 'active';
CREATE UNIQUE INDEX idx_accounts_nin_active ON accounts(nin) WHERE status = 'active';
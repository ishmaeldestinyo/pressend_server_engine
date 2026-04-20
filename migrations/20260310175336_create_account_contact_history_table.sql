CREATE TABLE IF NOT EXISTS account_contact_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    field VARCHAR(20) NOT NULL,
    old_value VARCHAR(255) NOT NULL,
    new_value VARCHAR(255) NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_at TIMESTAMPTZ,
    CONSTRAINT unique_contact_change UNIQUE (account_id, field, old_value, new_value)
);

CREATE INDEX idx_contact_history_account_id ON account_contact_history(account_id);
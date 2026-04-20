DROP TABLE IF EXISTS transactions CASCADE;

CREATE TABLE transactions (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id               UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    reciever_id             UUID        REFERENCES accounts(id) ON DELETE SET NULL,

    -- populated only when reciever_id is NULL (external transfer)
    reciever_account_number TEXT,
    reciever_account_name   TEXT,
    reciever_bank           TEXT,

    reference               TEXT        NOT NULL UNIQUE,
    type                    TEXT        NOT NULL CHECK (type IN ('debit', 'credit')),
    amount                  NUMERIC     NOT NULL CHECK (amount > 0),
    currency                TEXT        NOT NULL DEFAULT 'NGN',
    narration               TEXT,
    status                  TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'success', 'failed', 'reversed')),
    channel                 TEXT        CHECK (channel IN ('internal', 'external', 'vas')),
    balance_before          NUMERIC,
    balance_after           NUMERIC,
    meta                    JSONB,
    resolution_id           UUID,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_reciever CHECK (
        reciever_id IS NOT NULL
        OR (
            reciever_account_number IS NOT NULL AND
            reciever_account_name   IS NOT NULL AND
            reciever_bank           IS NOT NULL
        )
    )
);

CREATE INDEX idx_transactions_sender_id               ON transactions(sender_id);
CREATE INDEX idx_transactions_reciever_id             ON transactions(reciever_id);
CREATE INDEX idx_transactions_reciever_account_number ON transactions(reciever_account_number);
CREATE INDEX idx_transactions_reference               ON transactions(reference);
CREATE INDEX idx_transactions_status                  ON transactions(status);
CREATE INDEX idx_transactions_created_at              ON transactions(created_at);

CREATE TABLE resolutions (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID        NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    reason          TEXT        NOT NULL,
    resolved_by     UUID        REFERENCES accounts(id) ON DELETE SET NULL,
    resolution_note TEXT,
    status          TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_resolutions_transaction_id ON resolutions(transaction_id);

ALTER TABLE transactions
    ADD CONSTRAINT fk_transactions_resolution
    FOREIGN KEY (resolution_id) REFERENCES resolutions(id) ON DELETE SET NULL;

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS resolution_id UUID REFERENCES resolutions(id) ON DELETE SET NULL;
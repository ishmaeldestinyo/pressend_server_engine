CREATE TABLE IF NOT EXISTS vas_transactions (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id            UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,

    -- vas type
    vas_type              TEXT        NOT NULL CHECK (vas_type IN ('airtime', 'data', 'cable', 'power')),

    -- network/provider
    network               TEXT,                  -- MTN, AIRTEL, GLO, 9MOBILE
    provider              TEXT,                  -- for cable: DSTV, GOTV | power: IKEDC, EKEDC etc

    -- recipient
    recipient             TEXT        NOT NULL,  -- phone number, meter number, smartcard number

    -- transaction details
    amount                NUMERIC     NOT NULL CHECK (amount > 0),
    currency              TEXT        NOT NULL DEFAULT 'NGN',
    reference             TEXT        NOT NULL UNIQUE,
    debit_account         TEXT        NOT NULL,  -- account number debited

    -- status tracking
    status                TEXT        NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending', 'success', 'failed')),
    response_code         TEXT,                  -- VAS response code
    response_message      TEXT,                  -- VAS response message

    -- for success rate calculation per network
    -- quick query: SELECT network, COUNT(*), SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) FROM vas_transactions GROUP BY network
    meta                  JSONB,                 -- extra data e.g data plan, cable package etc

    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_vas_transactions_account_id  ON vas_transactions(account_id);
CREATE INDEX idx_vas_transactions_vas_type    ON vas_transactions(vas_type);
CREATE INDEX idx_vas_transactions_network     ON vas_transactions(network);
CREATE INDEX idx_vas_transactions_status      ON vas_transactions(status);
CREATE INDEX idx_vas_transactions_reference   ON vas_transactions(reference);
CREATE INDEX idx_vas_transactions_created_at  ON vas_transactions(created_at);

-- composite index for success rate queries per network
CREATE INDEX idx_vas_transactions_network_status ON vas_transactions(network, status);
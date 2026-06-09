-- Add migration script here
CREATE TABLE IF NOT EXISTS referral_rewards (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    referrer_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount          NUMERIC     NOT NULL DEFAULT 1000,
    currency        TEXT        NOT NULL DEFAULT 'NGN',
    referral_count  INT         NOT NULL DEFAULT 5,
    status          TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'paid', 'failed')),
    transaction_id  UUID        REFERENCES transactions(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_referral_rewards_referrer_id ON referral_rewards(referrer_id);
CREATE INDEX idx_referral_rewards_status      ON referral_rewards(status);
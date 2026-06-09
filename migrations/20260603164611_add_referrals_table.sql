CREATE TABLE IF NOT EXISTS referrals (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    referrer_id         UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    referred_id         UUID        NOT NULL UNIQUE REFERENCES accounts(id) ON DELETE CASCADE,

    status              TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN (
                                'pending',
                                'disqualified',
                                'palm_done',
                                'qualified'
                            )),

    disqualified_reason TEXT,
    palm_transfer_at    TIMESTAMPTZ,
    vas_payment_at      TIMESTAMPTZ,
    qualified_at        TIMESTAMPTZ,
    rewarded            BOOLEAN     NOT NULL DEFAULT FALSE,
    rewarded_at         TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_referrals_referrer_id ON referrals(referrer_id);
CREATE INDEX idx_referrals_referred_id ON referrals(referred_id);
CREATE INDEX idx_referrals_status      ON referrals(status);
CREATE INDEX idx_referrals_rewarded    ON referrals(rewarded);
CREATE INDEX idx_referrals_referrer_qualified_unrewarded
    ON referrals(referrer_id, rewarded)
    WHERE qualified_at IS NOT NULL;

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
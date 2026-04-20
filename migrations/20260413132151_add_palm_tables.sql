-- palm_enrollments
CREATE TABLE IF NOT EXISTS palm_enrollments (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    palm_type      TEXT        NOT NULL CHECK (palm_type IN ('main', 'panic')),
    feature_vector FLOAT8[]    NOT NULL,
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    enrolled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_palm_enrollments_account_id
    ON palm_enrollments(account_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_palm_enrollments_unique_active
    ON palm_enrollments(account_id, palm_type)
    WHERE is_active = TRUE;


-- palm_payment_verifications
CREATE TABLE IF NOT EXISTS palm_payment_verifications (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    payment_reference TEXT        NOT NULL UNIQUE,
    palm_type_matched TEXT        CHECK (palm_type_matched IN ('main', 'panic')),
    similarity_score  FLOAT8,
    is_successful     BOOLEAN     NOT NULL DEFAULT FALSE,
    is_panic          BOOLEAN     NOT NULL DEFAULT FALSE,
    confirmed_at      TIMESTAMPTZ,
    failed_at         TIMESTAMPTZ,
    failure_reason    TEXT        CHECK (failure_reason IN ('no_match', 'liveness_failed', 'not_enrolled')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_palm_payment_verifications_account_id
    ON palm_payment_verifications(account_id);
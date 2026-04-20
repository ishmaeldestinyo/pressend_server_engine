ALTER TABLE palm_enrollments
    ADD COLUMN IF NOT EXISTS hand_side VARCHAR(5) NOT NULL DEFAULT 'right'
    CHECK (hand_side IN ('left', 'right'));

ALTER TABLE palm_payment_verifications
    ADD COLUMN IF NOT EXISTS hand_side VARCHAR(5)
    CHECK (hand_side IN ('left', 'right'));
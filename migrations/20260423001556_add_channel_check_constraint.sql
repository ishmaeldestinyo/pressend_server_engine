ALTER TABLE transactions
    DROP CONSTRAINT IF EXISTS transactions_channel_check;

ALTER TABLE transactions
    ADD CONSTRAINT transactions_channel_check
    CHECK (channel IN ('internal', 'external', 'vas', 'palm'));
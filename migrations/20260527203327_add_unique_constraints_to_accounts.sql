-- Add migration script here
ALTER TABLE accounts
    ADD CONSTRAINT accounts_nin_unique UNIQUE (nin),
    ADD CONSTRAINT accounts_bvn_unique UNIQUE (bvn),
    ADD CONSTRAINT accounts_phone_number_unique UNIQUE (phone_number);
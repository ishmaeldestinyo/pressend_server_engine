-- Add migration script here
-- Allow NULL for both to support external inflows and system fees
ALTER TABLE transactions ALTER COLUMN sender_id DROP NOT NULL;
ALTER TABLE transactions ALTER COLUMN reciever_id DROP NOT NULL;
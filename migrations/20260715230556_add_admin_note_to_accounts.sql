-- Add migration script here
ALTER TABLE accounts ADD COLUMN admin_note TEXT;
ALTER TABLE accounts ADD COLUMN admin_note_updated_at TIMESTAMPTZ;
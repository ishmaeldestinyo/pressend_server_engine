-- Add migration script here
CREATE INDEX IF NOT EXISTS idx_accounts_device_token 
ON accounts(device_token) 
WHERE device_token IS NOT NULL AND status = 'active';
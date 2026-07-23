-- Add migration script here
-- Create Enum for Push Notification Status
CREATE TYPE push_notification_status AS ENUM (
    'draft',
    'scheduled',
    'processing',
    'sent',
    'failed',
    'cancelled'
);

-- Create Push Notifications Table
CREATE TABLE IF NOT EXISTS push_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    data JSONB DEFAULT '{}'::jsonb,
    target_group VARCHAR(100) NOT NULL DEFAULT 'all', -- 'all', 'topic_name', 'user_segment'
    status push_notification_status NOT NULL DEFAULT 'draft',
    scheduled_at TIMESTAMPTZ NULL, -- NULL = instant dispatch, populated = scheduled
    sent_at TIMESTAMPTZ NULL,
    total_recipients INT DEFAULT 0,
    successful_sends INT DEFAULT 0,
    failed_sends INT DEFAULT 0,
    error_log TEXT NULL,
    created_by UUID NULL, -- ID of the admin who created it
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast cron/worker polling on scheduled notifications
CREATE INDEX idx_push_notifications_status_scheduled 
ON push_notifications (status, scheduled_at) 
WHERE status = 'scheduled';

-- Automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_push_notifications_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_push_notifications_updated_at
BEFORE UPDATE ON push_notifications
FOR EACH ROW
EXECUTE FUNCTION update_push_notifications_updated_at();
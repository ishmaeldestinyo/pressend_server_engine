-- Note: No schema change needed, status is already VARCHAR(50)
-- Adding index to speed up cron queries filtering by partial_failure status

CREATE INDEX IF NOT EXISTS idx_legacy_plans_partial_failure
    ON legacy_plans (status, executed_at, deleted_at, last_activity_at)
    WHERE deleted_at IS NULL AND status = 'partial_failure';
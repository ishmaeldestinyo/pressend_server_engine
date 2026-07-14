-- Add migration script here
CREATE TABLE IF NOT EXISTS admins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    firstname VARCHAR(100) NOT NULL,
    lastname VARCHAR(100) NOT NULL,

    password_hash VARCHAR(255) NOT NULL,
    pin_hash VARCHAR(255),

    is_2fa_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    two_2fa_secret VARCHAR(255),
    device_id VARCHAR(255),
    device_token VARCHAR(255),

    phone_number VARCHAR(15),
    phone_no_verified BOOLEAN DEFAULT FALSE,

    -- Role / access control
    role VARCHAR(30) NOT NULL DEFAULT 'admin', -- e.g. super_admin, admin, support, compliance
    permissions JSONB NOT NULL DEFAULT '[]',

    status VARCHAR(20) NOT NULL DEFAULT 'active', -- active, suspended, disabled

    -- Invitation / creation trail
    invited_by UUID REFERENCES admins(id),
    last_login_at TIMESTAMPTZ,
    last_login_ip VARCHAR(45),

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    deleted_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_admins_email ON admins(email);
CREATE INDEX IF NOT EXISTS idx_admins_phone ON admins(phone_number);
CREATE INDEX IF NOT EXISTS idx_admins_status ON admins(status);
CREATE INDEX IF NOT EXISTS idx_admins_role ON admins(role);
CREATE INDEX IF NOT EXISTS idx_admins_lastname ON admins(lastname);
CREATE INDEX IF NOT EXISTS idx_admins_firstname ON admins(firstname);
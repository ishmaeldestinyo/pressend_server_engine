CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    firstname VARCHAR(100),
    othername VARCHAR(100),
    lastname VARCHAR(100),
    lga VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    is_2fa_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    two_2fa_secret VARCHAR(255),
    device_id VARCHAR(255),
    device_token VARCHAR(255),

    phone_number VARCHAR(15),
    phone_no_verified BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    password_hash VARCHAR(255) NOT NULL,
    pin_hash VARCHAR(255),
    account_type VARCHAR(20) NOT NULL,

    -- KYC tiers
    current_tier SMALLINT NOT NULL DEFAULT 0,
    pending_tier_upgrade SMALLINT,
    tier_upgraded_at TIMESTAMPTZ,
    tier_upgrade_requested_at TIMESTAMPTZ,

    -- Panic fields
    panic_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    panic_message TEXT,
    panic_activated_at TIMESTAMPTZ,
    panic_deactivated_at TIMESTAMPTZ,

    -- 9PSB fields
    account_number VARCHAR(11) UNIQUE,
    account_name VARCHAR(255),
    bank_name VARCHAR(255),

    -- KYC tier 1
    bvn VARCHAR(11) UNIQUE,
    nin VARCHAR(11) UNIQUE,
    nin_userid VARCHAR(20),
    gender VARCHAR(10),
    date_of_birth VARCHAR(10),
    address VARCHAR(255),

    -- KYC tier 2/3
    -- (state, city, lga reused from top-level columns)
    id_type SMALLINT,
    id_number VARCHAR(50),
    id_issue_date VARCHAR(10),
    id_expiry_date VARCHAR(10),
    house_number VARCHAR(50),
    street_name VARCHAR(255),
    nearest_landmark VARCHAR(255),
    place_of_birth VARCHAR(100),
    pep VARCHAR(3),

    -- KYC tier 2/3 documents
    user_photo TEXT,
    id_card_front TEXT,
    id_card_back TEXT,
    customer_signature TEXT,
    utility_bill TEXT,
    proof_of_address TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    deleted_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email);
CREATE INDEX IF NOT EXISTS idx_accounts_phone ON accounts(phone_number);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_firstname ON accounts(firstname);
CREATE INDEX IF NOT EXISTS idx_accounts_lastname ON accounts(lastname);
CREATE INDEX IF NOT EXISTS idx_accounts_othername ON accounts(othername);
CREATE INDEX IF NOT EXISTS idx_accounts_account_number ON accounts(account_number);
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    email_verified BOOLEAN NOT NULL DEFAULT FALSE,
    firstname VARCHAR(100),
    othername VARCHAR(100),
    lastname VARCHAR(100),
    is_2fa_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    two_2fa_secret VARCHAR(255),
    device_id VARCHAR(255), -- hardware ID, your existing column, used for new device detection
    device_token VARCHAR(255), --  FCM token, new column, used ONLY for push notifications

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

    -- KYC tier 1 fields
    -- (firstname, lastname, othername, phone_number already exist above)
    bvn VARCHAR(11),
    nin VARCHAR(11),
    nin_userid VARCHAR(20),         -- ABCDEF-0123 format
    gender VARCHAR(10),             -- MALE or FEMALE
    date_of_birth VARCHAR(10),      -- dd/MM/yyyy
    address VARCHAR(255),

    -- KYC tier 2/3 fields
    -- (bvn, nin, phone_number already exist above — reused from tier 1)
    id_type SMALLINT,               -- 1=NIN, 2=Driver's License, 3=Voter's Card, 4=Passport
    id_number VARCHAR(50),
    id_issue_date VARCHAR(10),      -- yyyy-MM-dd
    id_expiry_date VARCHAR(10),     -- yyyy-MM-dd, optional for NIN
    house_number VARCHAR(50),
    street_name VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    local_government VARCHAR(100),
    nearest_landmark VARCHAR(255),
    place_of_birth VARCHAR(100),
    pep VARCHAR(3),                 -- YES or NO

    -- KYC tier 2/3 documents (Base64 — large, stored as TEXT)
    user_photo TEXT,
    id_card_front TEXT,
    id_card_back TEXT,
    customer_signature TEXT,
    utility_bill TEXT,
    proof_of_address TEXT,          -- optional for tier 2, mandatory for tier 3

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    deleted_reason TEXT
);

CREATE INDEX idx_accounts_email ON accounts(email);
CREATE INDEX idx_accounts_phone ON accounts(phone_number);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_firstname ON accounts(firstname);
CREATE INDEX idx_accounts_lastname ON accounts(lastname);
CREATE INDEX idx_accounts_othername ON accounts(othername);
CREATE INDEX idx_accounts_account_number ON accounts(account_number);
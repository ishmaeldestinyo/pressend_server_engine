-- Add migration script here
ALTER TABLE kyc_verifications DROP CONSTRAINT kyc_verifications_reference_id_key;
CREATE INDEX idx_kyc_verifications_reference_id ON kyc_verifications(reference_id);
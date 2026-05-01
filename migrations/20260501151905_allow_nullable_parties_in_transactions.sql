ALTER TABLE transactions DROP CONSTRAINT IF EXISTS chk_reciever;

ALTER TABLE transactions 
ADD CONSTRAINT chk_reciever_flexible CHECK (
    reciever_id IS NOT NULL 
    OR channel = 'internal' 
    OR (
        reciever_account_number IS NOT NULL AND 
        reciever_account_name IS NOT NULL AND 
        reciever_bank IS NOT NULL
    )
);
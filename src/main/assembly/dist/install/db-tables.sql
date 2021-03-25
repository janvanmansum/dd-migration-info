CREATE TABLE IF NOT EXISTS data_file (
    storage_identifier VARCHAR(60) NOT NULL PRIMARY KEY,
    dataset_doi VARCHAR(100) NOT NULL,
    file_name VARCHAR(1000) NOT NULL, -- remove?
    mime_type VARCHAR(50) NOT NULL,
    sha1_checksum CHAR(40) NOT NULL,
    file_size BIGINT NOT NULL);

GRANT INSERT, SELECT, UPDATE, DELETE ON data_file TO dd_migration_info;

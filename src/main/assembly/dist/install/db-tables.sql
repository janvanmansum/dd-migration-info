CREATE TABLE IF NOT EXISTS basic_file_metadata (
    storage_identifier VARCHAR(60) UNIQUE NOT NULL,
    dataset_doi VARCHAR(100) NOT NULL,
    version_sequence_number INTEGER NOT NULL,
    file_name VARCHAR(1000) NOT NULL,
    directory_label VARCHAR(1000),
    mime_type VARCHAR(50) NOT NULL,
    sha1_checksum CHAR(40) NOT NULL,
    file_size BIGINT NOT NULL,
    PRIMARY KEY (storage_identifier, dataset_doi));

GRANT INSERT, SELECT, UPDATE, DELETE ON basic_file_metadata TO dd_migration_info;

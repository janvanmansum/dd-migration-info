CREATE TABLE IF NOT EXISTS data_file (
    storage_identifier CHAR(60) NOT NULL PRIMARY KEY,
    file_name VARCHAR(1000) NOT NULL,
    mime_type VARCHAR(50) NOT NULL,
    sha1_checksum CHAR(40) NOT NULL,
    file_size BIGINT NOT NULL);

GRANT INSERT, SELECT, UPDATE, DELETE ON data_file TO dd_migration_info;

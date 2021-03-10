CREATE TABLE IF NOT EXISTS objectstore_file (
    storage_id CHAR(28) NOT NULL PRIMARY KEY,
    datastation CHAR(20) NOT NULL,
    doi CHAR(40) NOT NULL,
    sha1sum CHAR(40) NOT NULL

GRANT INSERT, SELECT, UPDATE, DELETE ON objectstore_file TO dd_migration_info;

-- Information only, not a test of the extension: export the DuckLake tables from test.sql to Delta
-- with the community delta_export extension and compare row counts. Written to delta_export_info.md.

INSTALL azure;
LOAD azure;
INSTALL ducklake;
LOAD ducklake;
INSTALL delta_export FROM community;
LOAD delta_export;
INSTALL delta;
LOAD delta;
LOAD duckdb_object_storage;

SET GLOBAL azure_transport_option_type = 'curl';
CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getenv('ONELAKE_TOKEN'));
SET duckdb_objfs_backend = 'azure';
SET duckdb_objfs_bucket = 'mimtest';
SET duckdb_objfs_root = 'duckdb_objfs.Lakehouse/Files/duckdb_objfs';

ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (
    METADATA_CATALOG '__ducklake_metadata_lake'  -- delta_export() finds the catalog through this
);
CALL delta_export();  -- writes each table's _delta_log next to its Parquet

.mode markdown
.output delta_export_info.md
SELECT 'test_sales' AS "table",
       (SELECT count(*) FROM lake.ducklake_test.test_sales) AS ducklake_rows,
       (SELECT count(*) FROM delta_scan('abfss://mimtest@onelake.dfs.fabric.microsoft.com/duckdb_objfs.Lakehouse/Tables/ducklake_test/test_sales')) AS delta_rows
UNION ALL
SELECT 'test_customers_v2',
       (SELECT count(*) FROM lake.ducklake_test.test_customers_v2),
       (SELECT count(*) FROM delta_scan('abfss://mimtest@onelake.dfs.fabric.microsoft.com/duckdb_objfs.Lakehouse/Tables/ducklake_test/test_customers'));
.output

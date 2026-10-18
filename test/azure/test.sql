-- DuckLake on OneLake: catalog in a duckdb_objfs database, data in the lakehouse Tables/ section.
-- Run with: ONELAKE_TOKEN=<token for https://storage.azure.com> duckdb -bail -f test/azure/test.sql
-- One catalog and one schema, reused by every run. Results: checks table, also written to ducklake_checks.md.
-- Delta export is not part of this test; see delta_export_info.sql.

.timer on
INSTALL azure;
LOAD azure;
INSTALL ducklake;
LOAD ducklake;
LOAD duckdb_object_storage;

SET GLOBAL azure_transport_option_type = 'curl';
CREATE OR REPLACE SECRET onelake (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getenv('ONELAKE_TOKEN'));
SET duckdb_objfs_backend = 'azure';
SET duckdb_objfs_bucket = 'mimtest';
SET duckdb_objfs_root = 'duckdb_objfs.Lakehouse/Files/duckdb_objfs';

CREATE TEMP TABLE checks (check_name VARCHAR, expected VARCHAR, actual VARCHAR);

ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (
    DATA_PATH 'abfss://mimtest@onelake.dfs.fabric.microsoft.com/duckdb_objfs.Lakehouse/Tables/',
    DATA_INLINING_ROW_LIMIT 0  -- every row in Parquet
);
CREATE SCHEMA IF NOT EXISTS lake.ducklake_test;
USE lake.ducklake_test;

---------------------------------------------------------------- test_sales: DML, time travel, merge

CREATE OR REPLACE TABLE test_sales (
    sale_id BIGINT, sale_date DATE, sale_ts TIMESTAMP, region VARCHAR, product VARCHAR,
    quantity INTEGER, unit_price DECIMAL(18, 4), discount DOUBLE, is_online BOOLEAN
);
ALTER TABLE test_sales SET PARTITIONED BY (region);

CREATE OR REPLACE TEMP MACRO sales_batch(lo, hi) AS TABLE
    SELECT range AS sale_id,
           DATE '2024-01-01' + (range % 730)::INTEGER AS sale_date,
           TIMESTAMP '2024-01-01' + to_seconds(range * 37) AS sale_ts,
           ['north', 'south', 'east', 'west'][1 + range % 4] AS region,
           'product_' || (range % 250) AS product,
           (1 + range % 20)::INTEGER AS quantity,
           (5 + (range % 1000) * 0.37)::DECIMAL(18, 4) AS unit_price,
           (range % 15) / 100.0 AS discount,
           range % 3 = 0 AS is_online
    FROM range(lo, hi);

INSERT INTO test_sales SELECT * FROM sales_batch(0, 100000);
SET VARIABLE first_insert_snapshot = (SELECT max(snapshot_id) FROM ducklake_snapshots('lake'));
SET VARIABLE first_insert_time = (SELECT max(snapshot_time) FROM ducklake_snapshots('lake'));

INSERT INTO test_sales SELECT * FROM sales_batch(100000, 150000);
UPDATE test_sales SET discount = 0.5 WHERE region = 'west' AND quantity = 20;
DELETE FROM test_sales WHERE sale_id % 97 = 0;  -- 1547 rows
SET VARIABLE delete_snapshot = (SELECT max(snapshot_id) FROM ducklake_snapshots('lake'));

BEGIN;
DELETE FROM test_sales;
ROLLBACK;
INSERT INTO checks SELECT 'rollback leaves rows', '148453', count(*)::VARCHAR FROM test_sales;

ALTER TABLE test_sales ADD COLUMN channel VARCHAR;
INSERT INTO test_sales SELECT *, CASE WHEN is_online THEN 'web' ELSE 'store' END FROM sales_batch(150000, 160000);

-- Upsert: update ids 1000..1499 that still exist (495); insert the 5 deleted ones and 160000..160499 (505).
MERGE INTO test_sales AS target
USING (SELECT * FROM sales_batch(1000, 1500) UNION ALL SELECT * FROM sales_batch(160000, 160500)) AS source
ON target.sale_id = source.sale_id
WHEN MATCHED THEN UPDATE SET quantity = 999
WHEN NOT MATCHED THEN INSERT VALUES (source.sale_id, source.sale_date, source.sale_ts, source.region,
    source.product, source.quantity, source.unit_price, source.discount, source.is_online, 'merge');

CREATE OR REPLACE VIEW test_sales_by_region AS
SELECT region, count(*) AS sales, sum(quantity * unit_price) AS revenue FROM test_sales GROUP BY region;

---------------------------------------------------------------- test_customers: DDL

DROP TABLE IF EXISTS test_customers_v2;  -- last run's renamed table
CREATE OR REPLACE TABLE test_customers (
    customer_id INTEGER, customer_name VARCHAR, country VARCHAR, segment VARCHAR,
    signup_date DATE, lifetime_value DOUBLE
);
ALTER TABLE test_customers SET PARTITIONED BY (country);
INSERT INTO test_customers
SELECT range::INTEGER, 'customer_' || range, ['AU', 'NZ', 'FR', 'DZ', 'US'][1 + range % 5],
       ['retail', 'smb', 'enterprise'][1 + range % 3], DATE '2020-01-01' + (range % 2000)::INTEGER,
       (range % 5000) * 1.5
FROM range(20000);
SET VARIABLE before_ddl_snapshot = (SELECT max(snapshot_id) FROM ducklake_snapshots('lake'));

ALTER TABLE test_customers RENAME COLUMN customer_name TO full_name;
ALTER TABLE test_customers ALTER COLUMN customer_id SET DATA TYPE BIGINT;
ALTER TABLE test_customers DROP COLUMN segment;
ALTER TABLE test_customers SET PARTITIONED BY (country, year(signup_date));
INSERT INTO test_customers (customer_id, full_name, country, signup_date, lifetime_value)
SELECT range, 'customer_' || range, ['AU', 'NZ', 'FR', 'DZ', 'US'][1 + range % 5],
       DATE '2020-01-01' + (range % 2000)::INTEGER, (range % 5000) * 1.5
FROM range(20000, 25000);
ALTER TABLE test_customers RENAME TO test_customers_v2;

---------------------------------------------------------------- reopen read-only and check

USE memory;
DETACH lake;
ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake (READ_ONLY);
USE lake.ducklake_test;

INSERT INTO checks SELECT 'test_sales rows', '158958', count(*)::VARCHAR FROM test_sales;
INSERT INTO checks SELECT 'time travel by version', '100000', count(*)::VARCHAR
FROM test_sales AT (VERSION => getvariable('first_insert_snapshot'));
INSERT INTO checks SELECT 'time travel by timestamp', '100000', count(*)::VARCHAR
FROM test_sales AT (TIMESTAMP => getvariable('first_insert_time'));
INSERT INTO checks SELECT 'change feed: first insert', '100000', count(*)::VARCHAR
FROM ducklake_table_changes('lake', 'ducklake_test', 'test_sales',
                            getvariable('first_insert_snapshot'), getvariable('first_insert_snapshot'))
WHERE change_type = 'insert';
INSERT INTO checks SELECT 'change feed: delete', '1547', count(*)::VARCHAR
FROM ducklake_table_changes('lake', 'ducklake_test', 'test_sales',
                            getvariable('delete_snapshot'), getvariable('delete_snapshot'))
WHERE change_type = 'delete';
INSERT INTO checks SELECT 'update applied', 'true', (count(*) > 0)::VARCHAR FROM test_sales WHERE discount = 0.5;
INSERT INTO checks SELECT 'merge inserted', '505', count(*)::VARCHAR FROM test_sales WHERE channel = 'merge';
INSERT INTO checks SELECT 'merge updated', '495', count(*)::VARCHAR
FROM test_sales WHERE sale_id BETWEEN 1000 AND 1499 AND quantity = 999;
INSERT INTO checks SELECT 'added column filled', '10505', count(channel)::VARCHAR FROM test_sales;
INSERT INTO checks SELECT 'partitions (region)', 'east,north,south,west',
    string_agg(DISTINCT regexp_extract(data_file, 'region=([^/]+)', 1), ','
               ORDER BY regexp_extract(data_file, 'region=([^/]+)', 1))
FROM ducklake_list_files('lake', 'test_sales', schema => 'ducklake_test');
INSERT INTO checks SELECT 'view', '4', count(*)::VARCHAR FROM test_sales_by_region;

INSERT INTO checks SELECT 'renamed table rows', '25000', count(*)::VARCHAR FROM test_customers_v2;
INSERT INTO checks SELECT 'old table name gone', '0', count(*)::VARCHAR
FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'ducklake_test' AND table_name = 'test_customers';
INSERT INTO checks SELECT 'renamed column', 'full_name', string_agg(column_name, ',')
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name LIKE '%name';
INSERT INTO checks SELECT 'dropped column', '0', count(*)::VARCHAR
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name = 'segment';
INSERT INTO checks SELECT 'type promotion', 'BIGINT', data_type
FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'test_customers_v2' AND column_name = 'customer_id';
INSERT INTO checks SELECT 'partition evolution (levels per file)', '1,2',
    string_agg(DISTINCT len(regexp_extract_all(data_file, '/[^/=]+=[^/]+'))::VARCHAR, ','
               ORDER BY len(regexp_extract_all(data_file, '/[^/=]+=[^/]+'))::VARCHAR)
FROM ducklake_list_files('lake', 'test_customers_v2', schema => 'ducklake_test');
INSERT INTO checks SELECT 'time travel before rename (old name)', '20000', count(*)::VARCHAR
FROM test_customers AT (VERSION => getvariable('before_ddl_snapshot'));
INSERT INTO checks SELECT 'time travel sees dropped column', '3', count(DISTINCT segment)::VARCHAR
FROM test_customers AT (VERSION => getvariable('before_ddl_snapshot'));

---------------------------------------------------------------- maintenance

USE memory;
DETACH lake;
ATTACH 'ducklake:duckdb_objfs://catalog.ducklake' AS lake;
USE lake.ducklake_test;

CALL ducklake_rewrite_data_files('lake');  -- folds delete files into new data files
INSERT INTO checks SELECT 'rows after rewriting deletes', '158958', count(*)::VARCHAR FROM test_sales;
SET VARIABLE files_before = (SELECT count(*) FROM ducklake_list_files('lake', 'test_sales', schema => 'ducklake_test'));
CALL ducklake_merge_adjacent_files('lake');
INSERT INTO checks SELECT 'compaction (data files)', 'fewer',
    CASE WHEN count(*) < getvariable('files_before') THEN 'fewer'
         ELSE getvariable('files_before')::VARCHAR || ' -> ' || count(*)::VARCHAR END
FROM ducklake_list_files('lake', 'test_sales', schema => 'ducklake_test');
CALL ducklake_expire_snapshots('lake', older_than => now());
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);
INSERT INTO checks SELECT 'snapshots after expire', '1', count(*)::VARCHAR FROM ducklake_snapshots('lake');
INSERT INTO checks SELECT 'rows after maintenance', '158958', count(*)::VARCHAR FROM test_sales;

---------------------------------------------------------------- results

USE memory;
DETACH lake;
.mode markdown
.output ducklake_checks.md
SELECT check_name AS "check", expected, actual,
       CASE WHEN expected IS NOT DISTINCT FROM actual THEN '✅' ELSE '❌' END AS result
FROM checks ORDER BY rowid;
.output
.mode duckbox
SELECT check_name, expected, actual, expected IS NOT DISTINCT FROM actual AS ok FROM checks ORDER BY rowid;
SELECT CASE WHEN count(*) > 0 THEN error('failed checks: ' || string_agg(check_name, ', ')) ELSE 'all checks passed' END
FROM checks WHERE expected IS DISTINCT FROM actual;

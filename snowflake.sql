-- ============================================================================
-- snowflake_setup.sql — Full Snowflake setup for the E-Commerce Data Lakehouse
--
-- Run this file top to bottom ONCE during initial setup.
-- For subsequent pipeline runs: run only the COPY INTO section at the bottom.
-- ============================================================================

-- ── Warehouse + Database ─────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS CAPSTONE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND   = 60
         AUTO_RESUME    = TRUE
         INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW;
USE DATABASE ECOMMERCE_DW;
USE SCHEMA PUBLIC;
USE WAREHOUSE CAPSTONE_WH;


-- ── Dimension tables (star schema) ───────────────────────────────────────────

CREATE OR REPLACE TABLE dim_customers (
    customer_id               VARCHAR,
    customer_unique_id        VARCHAR,
    customer_zip_code_prefix  VARCHAR,
    customer_city             VARCHAR,
    customer_state            VARCHAR
);

CREATE OR REPLACE TABLE dim_sellers (
    seller_id                VARCHAR,
    seller_zip_code_prefix   VARCHAR,
    seller_city              VARCHAR,
    seller_state             VARCHAR,
    seller_lat               FLOAT,
    seller_lng               FLOAT
);

CREATE OR REPLACE TABLE dim_products (
    product_id                       VARCHAR,
    product_category_name            VARCHAR,
    product_category_name_english    VARCHAR,
    product_name_length              INTEGER,
    product_description_length       INTEGER,
    product_photos_qty               INTEGER,
    product_weight_g                 FLOAT,
    product_length_cm                FLOAT,
    product_height_cm                FLOAT,
    product_width_cm                 FLOAT
);

CREATE OR REPLACE TABLE dim_geolocation (
    geolocation_zip_code_prefix  VARCHAR,
    geolocation_lat              FLOAT,
    geolocation_lng              FLOAT,
    geolocation_city             VARCHAR,
    geolocation_state            VARCHAR
);


-- ── Fact table ───────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE fact_order_items (
    order_id                     VARCHAR,
    order_item_id                INTEGER,
    customer_id                  VARCHAR,
    product_id                   VARCHAR,
    seller_id                    VARCHAR,
    price                        FLOAT,
    freight_value                FLOAT,
    total_amount                 FLOAT,
    payment_value                FLOAT,
    payment_type                 VARCHAR,
    avg_installments             FLOAT,
    order_status                 VARCHAR,
    purchase_timestamp           TIMESTAMP,
    delivered_date               TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    review_score                 FLOAT,
    delivery_days                INTEGER,
    delay_flag                   INTEGER,    -- NULL = not delivered, 1 = late, 0 = on time
    on_time_flag                 INTEGER     -- NULL = not delivered, 1 = on time, 0 = late
);


-- ── Business aggregate tables ────────────────────────────────────────────────

CREATE OR REPLACE TABLE order_summary (
    order_id        VARCHAR,
    total_items     INTEGER,
    total_price     FLOAT,
    total_freight   FLOAT,
    total_amount    FLOAT,
    order_status    VARCHAR,
    purchase_date   TIMESTAMP,
    delivered_date  TIMESTAMP,
    delivery_days   INTEGER
);

CREATE OR REPLACE TABLE customer_metrics (
    customer_id       VARCHAR,
    total_orders      INTEGER,
    total_spent       FLOAT,
    avg_order_value   FLOAT,
    last_order_date   TIMESTAMP,
    avg_review_score  FLOAT
);

CREATE OR REPLACE TABLE seller_performance (
    seller_id              VARCHAR,
    total_orders           INTEGER,
    total_revenue          FLOAT,
    avg_review_score       FLOAT,
    on_time_delivery_rate  FLOAT,
    avg_delivery_days      FLOAT
);

CREATE OR REPLACE TABLE product_performance (
    product_id        VARCHAR,
    total_quantity    INTEGER,
    total_sales       FLOAT,
    avg_price         FLOAT,
    avg_review_score  FLOAT
);

-- Verify all 9 tables were created
SHOW TABLES;


-- ── External Stage (Snowflake reads Parquet from S3) ─────────────────────────
-- Replace AWS_KEY_ID and AWS_SECRET_KEY with your NEW credentials
-- (rotate them first if you accidentally exposed the old ones)

CREATE OR REPLACE STAGE gold_s3_stage
    URL        = 's3://capstone-ecomm-team8/snowflake'
    CREDENTIALS = (
        AWS_KEY_ID     = ''
        AWS_SECRET_KEY = ''
    )
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Verify the stage can see your S3 folder
-- (run this AFTER at least one Gold pipeline run has completed)
LIST @gold_s3_stage;


-- ============================================================================
-- COPY INTO — run after EVERY pipeline execution to refresh Snowflake
-- FORCE = TRUE ensures re-runs always reload, even if files haven't changed.
-- TRUNCATE first so we don't accumulate duplicate rows across runs.
-- ============================================================================

-- Dims first (fact references them in BI joins)
TRUNCATE TABLE dim_customers;
COPY INTO dim_customers
    FROM @gold_s3_stage/dim_customers/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_sellers;
COPY INTO dim_sellers
    FROM @gold_s3_stage/dim_sellers/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_products;
COPY INTO dim_products
    FROM @gold_s3_stage/dim_products/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_geolocation;
COPY INTO dim_geolocation
    FROM @gold_s3_stage/dim_geolocation/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

-- Fact table (load after dims are ready)
TRUNCATE TABLE fact_order_items;
COPY INTO fact_order_items
    FROM @gold_s3_stage/fact_order_items/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

-- Aggregate tables (can load in parallel)
TRUNCATE TABLE order_summary;
COPY INTO order_summary
    FROM @gold_s3_stage/order_summary/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE customer_metrics;
COPY INTO customer_metrics
    FROM @gold_s3_stage/customer_metrics/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE seller_performance;
COPY INTO seller_performance
    FROM @gold_s3_stage/seller_performance/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

TRUNCATE TABLE product_performance;
COPY INTO product_performance
    FROM @gold_s3_stage/product_performance/
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FORCE = TRUE  ON_ERROR = 'CONTINUE';

-- ── Verify all 9 tables have data ────────────────────────────────────────────
SELECT 'dim_customers'       AS table_name, COUNT(*) AS row_count FROM dim_customers       UNION ALL
SELECT 'dim_sellers',                        COUNT(*)              FROM dim_sellers          UNION ALL
SELECT 'dim_products',                       COUNT(*)              FROM dim_products         UNION ALL
SELECT 'dim_geolocation',                    COUNT(*)              FROM dim_geolocation      UNION ALL
SELECT 'fact_order_items',                   COUNT(*)              FROM fact_order_items     UNION ALL
SELECT 'order_summary',                      COUNT(*)              FROM order_summary        UNION ALL
SELECT 'customer_metrics',                   COUNT(*)              FROM customer_metrics     UNION ALL
SELECT 'seller_performance',                 COUNT(*)              FROM seller_performance   UNION ALL
SELECT 'product_performance',                COUNT(*)              FROM product_performance
ORDER BY table_name;

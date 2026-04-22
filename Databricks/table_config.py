"""
table_config.py — Single source of truth for every table in the pipeline.

Design principle: to add a new table, add one dict entry here.
No other file needs to change. Everything reads from this config.

REFERENCE_TABLES  = slow-changing lookup data (customers, sellers, products...)
TRANSACTIONAL_TABLES = high-volume records that grow every batch (orders, items...)

These names are intentional — at Bronze/Silver layer these are just raw source
tables. The "dim_" and "fact_" naming only appears at the Gold layer where the
star schema is actually built.
"""

# ── Change this to match your S3 bucket name ──────────────────────────────────
S3_BUCKET   = "capstone-ecomm-team8"

# Use "s3" if Databricks was set up with CloudFormation (Premium trial).
# Use "s3a" if you configured access keys manually.
S3_PROTOCOL = "s3"

S3_BASE         = f"{S3_PROTOCOL}://{S3_BUCKET}"
S3_RAW          = f"{S3_BASE}/raw"
S3_LIVE         = f"{S3_BASE}/live"
S3_DELTA_BRONZE = f"{S3_BASE}/delta/bronze"
S3_DELTA_SILVER = f"{S3_BASE}/delta/silver"
S3_DELTA_GOLD   = f"{S3_BASE}/delta/gold"

# Separate Parquet export folder — Snowflake reads from here (not from Delta).
# Gold engine writes plain Parquet here after building each Gold table.
S3_SNOWFLAKE    = f"{S3_BASE}/snowflake"


# ── Reference tables (full load every pipeline run, loaded only in Batch 1) ───
REFERENCE_TABLES = {

    "category_translation": {
        "source_file"   : "product_category_name_translation.csv",
        "cleaning_rules": [],
        "merge_keys"    : ["product_category_name"],
        "dedup_keys"    : ["product_category_name"],
    },

    "geolocation": {
        "source_file"   : "geolocation_dataset.csv",
        "cleaning_rules": [
            {"column": "geolocation_zip_code_prefix", "action": "cast_string"},
        ],
        "merge_keys" : ["geolocation_zip_code_prefix"],
        "dedup_keys" : [],
        # Multiple lat/lng rows per zip — collapse to one row per zip
        "aggregate": {
            "group_by": ["geolocation_zip_code_prefix"],
            "aggs": {
                "geolocation_lat" : "avg",
                "geolocation_lng" : "avg",
                "geolocation_city": "first",
                "geolocation_state": "first",
            },
        },
    },

    "sellers": {
        "source_file"   : "sellers_dataset.csv",
        "cleaning_rules": [
            {"column": "seller_city",             "action": "initcap_trim"},
            {"column": "seller_state",            "action": "upper_trim"},
            {"column": "seller_zip_code_prefix",  "action": "cast_string"},
        ],
        "merge_keys": ["seller_id"],
        "dedup_keys": ["seller_id"],
    },

    "customers": {
        "source_file"   : "customers_dataset.csv",
        "cleaning_rules": [
            {"column": "customer_city",            "action": "initcap_trim"},
            {"column": "customer_state",           "action": "upper_trim"},
            {"column": "customer_zip_code_prefix", "action": "cast_string"},
        ],
        "merge_keys": ["customer_id"],
        "dedup_keys": ["customer_id"],
    },

    "products": {
        "source_file"   : "products_dataset.csv",
        "cleaning_rules": [
            # Fix the typos in the original Olist column names
            {"column": "product_name_lenght",        "action": "rename", "new_name": "product_name_length"},
            {"column": "product_description_lenght", "action": "rename", "new_name": "product_description_length"},
            {"column": "product_category_name",      "action": "fill_null", "default": "unknown"},
        ],
        "merge_keys": ["product_id"],
        "dedup_keys": ["product_id"],
        # Join with category_translation to get English category name
        "join": {
            "source_table" : "category_translation",
            "on"           : "product_category_name",
            "how"          : "left",
            "fill_after"   : {"product_category_name_english": "unknown"},
        },
    },
}


# ── Transactional tables (append new batches, grow over time) ─────────────────
TRANSACTIONAL_TABLES = {

    "orders": {
        "source_file"   : "orders_dataset.csv",
        "cleaning_rules": [
            {"column": "order_purchase_timestamp",       "action": "to_timestamp"},
            {"column": "order_approved_at",              "action": "to_timestamp"},
            {"column": "order_delivered_carrier_date",   "action": "to_timestamp"},
            {"column": "order_delivered_customer_date",  "action": "to_timestamp"},
            {"column": "order_estimated_delivery_date",  "action": "to_timestamp"},
        ],
        "merge_keys": ["order_id"],
        "dedup_keys": ["order_id"],
    },

    "order_items": {
        "source_file"   : "order_items_dataset.csv",
        "cleaning_rules": [
            {"column": "price",         "action": "cast_double"},
            {"column": "freight_value", "action": "cast_double"},
        ],
        "merge_keys": ["order_id", "order_item_id"],
        "dedup_keys": ["order_id", "order_item_id"],
    },

    "order_payments": {
        "source_file"   : "order_payments_dataset.csv",
        "cleaning_rules": [
            # "not_defined" in the raw data — replace with a clean unknown label
            {"column": "payment_type",         "action": "replace_value", "old": "not_defined", "new": "unknown"},
            {"column": "payment_value",        "action": "cast_double"},
            {"column": "payment_installments", "action": "cast_int"},
        ],
        "merge_keys": ["order_id", "payment_sequential"],
        "dedup_keys": [],
    },

    "order_reviews": {
        "source_file"   : "order_reviews_dataset.csv",
        "cleaning_rules": [
            {"column": "review_score",             "action": "cast_int"},
            {"column": "review_comment_title",     "action": "fill_null", "default": ""},
            {"column": "review_comment_message",   "action": "fill_null", "default": ""},
            {"column": "review_creation_date",     "action": "to_timestamp"},
            {"column": "review_answer_timestamp",  "action": "to_timestamp"},
        ],
        "merge_keys": ["review_id"],
        "dedup_keys": ["review_id"],
    },
}

# Convenience dict — used by Silver engine which processes everything at once
ALL_TABLES = {**REFERENCE_TABLES, **TRANSACTIONAL_TABLES}

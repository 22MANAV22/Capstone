"""
Table Configuration — Single source of truth.
To add a new table: add a dict entry. No other code changes needed.
"""

S3_BUCKET = "capstone-ecomm-team8"   # CHANGE THIS
S3_PROTOCOL = "s3"   # "s3" for Premium+CloudFormation, "s3a" for access keys

S3_BASE = f"{S3_PROTOCOL}://{S3_BUCKET}"
S3_RAW = f"{S3_BASE}/raw"
S3_LIVE = f"{S3_BASE}/live"
S3_DELTA_BRONZE = f"{S3_BASE}/delta/bronze"
S3_DELTA_SILVER = f"{S3_BASE}/delta/silver"
S3_DELTA_GOLD = f"{S3_BASE}/delta/gold"

DIMENSION_TABLES = {
    "category_translation": {
        "source_file": "product_category_name_translation.csv",
        "cleaning_rules": [],
        "merge_keys": ["product_category_name"],
        "dedup_keys": ["product_category_name"],
    },
    "geolocation": {
        "source_file": "geolocation_dataset.csv",
        "cleaning_rules": [
            {"column": "geolocation_zip_code_prefix", "action": "cast_string"},
        ],
        "merge_keys": ["geolocation_zip_code_prefix"],
        "dedup_keys": [],
        "aggregate": {
            "group_by": ["geolocation_zip_code_prefix"],
            "aggs": {
                "geolocation_lat": "avg", "geolocation_lng": "avg",
                "geolocation_city": "first", "geolocation_state": "first",
            },
        },
    },
    "sellers": {
        "source_file": "sellers_dataset.csv",
        "cleaning_rules": [
            {"column": "seller_city", "action": "initcap_trim"},
            {"column": "seller_state", "action": "upper_trim"},
            {"column": "seller_zip_code_prefix", "action": "cast_string"},
        ],
        "merge_keys": ["seller_id"],
        "dedup_keys": ["seller_id"],
    },
    "customers": {
        "source_file": "customers_dataset.csv",
        "cleaning_rules": [
            {"column": "customer_city", "action": "initcap_trim"},
            {"column": "customer_state", "action": "upper_trim"},
            {"column": "customer_zip_code_prefix", "action": "cast_string"},
        ],
        "merge_keys": ["customer_id"],
        "dedup_keys": ["customer_id"],
    },
    "products": {
        "source_file": "products_dataset.csv",
        "cleaning_rules": [
            {"column": "product_name_lenght", "action": "rename", "new_name": "product_name_length"},
            {"column": "product_description_lenght", "action": "rename", "new_name": "product_description_length"},
            {"column": "product_category_name", "action": "fill_null", "default": "unknown"},
        ],
        "merge_keys": ["product_id"],
        "dedup_keys": ["product_id"],
        "join": {
            "source_table": "category_translation",
            "on": "product_category_name", "how": "left",
            "fill_after": {"product_category_name_english": "unknown"},
        },
    },
}

FACT_TABLES = {
    "orders": {
        "source_file": "orders_dataset.csv",
        "cleaning_rules": [
            {"column": "order_purchase_timestamp", "action": "to_timestamp"},
            {"column": "order_approved_at", "action": "to_timestamp"},
            {"column": "order_delivered_carrier_date", "action": "to_timestamp"},
            {"column": "order_delivered_customer_date", "action": "to_timestamp"},
            {"column": "order_estimated_delivery_date", "action": "to_timestamp"},
        ],
        "merge_keys": ["order_id"],
        "dedup_keys": ["order_id"],
    },
    "order_items": {
        "source_file": "order_items_dataset.csv",
        "cleaning_rules": [
            {"column": "price", "action": "cast_double"},
            {"column": "freight_value", "action": "cast_double"},
        ],
        "merge_keys": ["order_id", "order_item_id"],
        "dedup_keys": ["order_id", "order_item_id"],
    },
    "order_payments": {
        "source_file": "order_payments_dataset.csv",
        "cleaning_rules": [
            {"column": "payment_type", "action": "replace_value", "old": "not_defined", "new": "unknown"},
            {"column": "payment_value", "action": "cast_double"},
            {"column": "payment_installments", "action": "cast_int"},
        ],
        "merge_keys": ["order_id", "payment_sequential"],
        "dedup_keys": [],
    },
    "order_reviews": {
        "source_file": "order_reviews_dataset.csv",
        "cleaning_rules": [
            {"column": "review_score", "action": "cast_int"},
            {"column": "review_comment_title", "action": "fill_null", "default": ""},
            {"column": "review_comment_message", "action": "fill_null", "default": ""},
            {"column": "review_creation_date", "action": "to_timestamp"},
            {"column": "review_answer_timestamp", "action": "to_timestamp"},
        ],
        "merge_keys": ["review_id"],
        "dedup_keys": ["review_id"],
    },
}

ALL_TABLES = {**DIMENSION_TABLES, **FACT_TABLES}
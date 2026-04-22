"""
gold_engine.py — Star schema + business aggregate tables.

CATALOG FIX:
  DROP TABLE IF EXISTS before every saveAsTable to clear any stale
  catalog entry. Same fix applied to Bronze and Silver.
"""

from pyspark.sql.functions import (
    col, count, countDistinct,
    sum  as _sum,
    avg  as _avg,
    max  as _max,
    min  as _min,
    round as _round,
    when, coalesce, lit, datediff
)
from table_config import S3_DELTA_SILVER, S3_DELTA_GOLD, S3_SNOWFLAKE


class GoldEngine:

    def __init__(self, spark):
        self.spark   = spark
        self.results = {}

    def _read_silver(self, name):
        return self.spark.read.format("delta").load(f"{S3_DELTA_SILVER}/{name}")

    def _read_gold(self, name):
        return self.spark.read.format("delta").load(f"{S3_DELTA_GOLD}/{name}")

    def _write_gold(self, df, name):
        """
        Write to Delta (Databricks catalog) AND Parquet (Snowflake).

        DROP TABLE IF EXISTS before saveAsTable — clears any stale catalog
        entry so the table is always created fresh with the correct schema.
        """
        delta_path   = f"{S3_DELTA_GOLD}/{name}"
        parquet_path = f"{S3_SNOWFLAKE}/{name}"

        # Clear stale catalog entry before writing
        self.spark.sql(f"DROP TABLE IF EXISTS gold.{name}")

        # Write Delta + register catalog atomically
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", delta_path)
            .option("overwriteSchema", "true")
            .saveAsTable(f"gold.{name}")
        )

        # Write Parquet separately for Snowflake COPY INTO
        df.write.format("parquet").mode("overwrite").save(parquet_path)

        c = df.count()
        self.results[name] = c
        print(f"    gold.{name} — {c:,} rows → Delta + Parquet")

    # ── Dimension tables ──────────────────────────────────────────────────────

    def build_dim_customers(self):
        print("\n  Building: dim_customers")
        df = self._read_silver("customers")
        self._write_gold(df, "dim_customers")

    def build_dim_sellers(self):
        """Sellers enriched with geolocation lat/lng via zip code join."""
        print("\n  Building: dim_sellers")
        sellers = self._read_silver("sellers")
        geo     = self._read_silver("geolocation")
        df = (
            sellers
            .join(
                geo,
                sellers.seller_zip_code_prefix == geo.geolocation_zip_code_prefix,
                "left"
            )
            .select(
                "seller_id", "seller_city", "seller_state",
                "seller_zip_code_prefix",
                col("geolocation_lat").alias("seller_lat"),
                col("geolocation_lng").alias("seller_lng"),
            )
        )
        self._write_gold(df, "dim_sellers")

    def build_dim_products(self):
        """Products already joined with category_translation in Silver."""
        print("\n  Building: dim_products")
        df = self._read_silver("products")
        self._write_gold(df, "dim_products")

    def build_dim_geolocation(self):
        """One row per zip code. Supports revenue-by-region KPI."""
        print("\n  Building: dim_geolocation")
        df = self._read_silver("geolocation")
        self._write_gold(df, "dim_geolocation")

    # ── Central fact table ────────────────────────────────────────────────────

    def build_fact_order_items(self):
        """
        fact_order_items — central star schema table.
        One row per order line item.
        delay_flag and on_time_flag are NULL for undelivered orders (not 0)
        to prevent skewing KPIs like on_time_delivery_rate.
        """
        print("\n  Building: fact_order_items")

        orders   = self._read_silver("orders")
        items    = self._read_silver("order_items")
        payments = self._read_silver("order_payments")
        reviews  = self._read_silver("order_reviews")

        pay_agg = payments.groupBy("order_id").agg(
            _sum("payment_value").alias("payment_value"),
            _max("payment_type").alias("payment_type"),
            _avg("payment_installments").alias("avg_installments"),
        )
        rev_agg = reviews.groupBy("order_id").agg(
            _avg("review_score").alias("review_score")
        )

        fact = (
            items.alias("oi")
            .join(orders.alias("o"),   "order_id")
            .join(pay_agg.alias("pa"), "order_id", "left")
            .join(rev_agg.alias("r"),  "order_id", "left")
            .select(
                col("oi.order_id"),
                col("oi.order_item_id"),
                col("o.customer_id"),
                col("oi.product_id"),
                col("oi.seller_id"),
                col("oi.price"),
                col("oi.freight_value"),
                (col("oi.price") + col("oi.freight_value")).alias("total_amount"),
                col("pa.payment_value"),
                col("pa.payment_type"),
                col("pa.avg_installments"),
                col("o.order_status"),
                col("o.order_purchase_timestamp").alias("purchase_timestamp"),
                col("o.order_delivered_customer_date").alias("delivered_date"),
                col("o.order_estimated_delivery_date"),
                coalesce(col("r.review_score"), lit(0.0)).alias("review_score"),
                datediff(
                    col("o.order_delivered_customer_date"),
                    col("o.order_purchase_timestamp")
                ).alias("delivery_days"),
                when(col("o.order_delivered_customer_date").isNull(), lit(None).cast("int"))
                .when(col("o.order_delivered_customer_date") > col("o.order_estimated_delivery_date"), 1)
                .otherwise(0).alias("delay_flag"),
                when(col("o.order_delivered_customer_date").isNull(), lit(None).cast("int"))
                .when(col("o.order_delivered_customer_date") <= col("o.order_estimated_delivery_date"), 1)
                .otherwise(0).alias("on_time_flag"),
            )
        )
        self._write_gold(fact, "fact_order_items")

    # ── Business aggregate tables ─────────────────────────────────────────────

    def build_order_summary(self):
        print("\n  Building: order_summary")
        f  = self._read_gold("fact_order_items")
        df = f.groupBy("order_id").agg(
            count("order_item_id").alias("total_items"),
            _round(_sum("price"),         2).alias("total_price"),
            _round(_sum("freight_value"), 2).alias("total_freight"),
            _round(_sum("total_amount"),  2).alias("total_amount"),
            _max("order_status").alias("order_status"),
            _min("purchase_timestamp").alias("purchase_date"),
            _max("delivered_date").alias("delivered_date"),
            _round(_avg("delivery_days"), 0).cast("int").alias("delivery_days"),
        )
        self._write_gold(df, "order_summary")

    def build_customer_metrics(self):
        """Customer Lifetime Value table."""
        print("\n  Building: customer_metrics")
        f  = self._read_gold("fact_order_items")
        df = f.groupBy("customer_id").agg(
            countDistinct("order_id").alias("total_orders"),
            _round(_sum("total_amount"), 2).alias("total_spent"),
            _round(_avg("total_amount"), 2).alias("avg_order_value"),
            _max("purchase_timestamp").alias("last_order_date"),
            _round(_avg("review_score"), 1).alias("avg_review_score"),
        )
        self._write_gold(df, "customer_metrics")

    def build_seller_performance(self):
        """on_time_delivery_rate excludes undelivered orders (NULL flags)."""
        print("\n  Building: seller_performance")
        f  = self._read_gold("fact_order_items")
        df = f.groupBy("seller_id").agg(
            countDistinct("order_id").alias("total_orders"),
            _round(_sum("price"),        2).alias("total_revenue"),
            _round(_avg("review_score"), 2).alias("avg_review_score"),
            _round(
                _sum("on_time_flag") * 100.0 /
                _sum(when(col("on_time_flag").isNotNull(), 1).otherwise(0)),
                1
            ).alias("on_time_delivery_rate"),
            _round(_avg(
                when(col("delivery_days").isNotNull(), col("delivery_days"))
            ), 1).alias("avg_delivery_days"),
        )
        self._write_gold(df, "seller_performance")

    def build_product_performance(self):
        print("\n  Building: product_performance")
        f  = self._read_gold("fact_order_items")
        df = f.groupBy("product_id").agg(
            count("*").alias("total_quantity"),
            _round(_sum("price"),        2).alias("total_sales"),
            _round(_avg("price"),        2).alias("avg_price"),
            _round(_avg("review_score"), 2).alias("avg_review_score"),
        )
        self._write_gold(df, "product_performance")

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self):
        """Build order: dims → fact → aggregates."""
        print("=" * 60)
        print("GOLD ENGINE — building star schema + business tables")
        print("=" * 60)
        self.results = {}

        self.build_dim_customers()
        self.build_dim_sellers()
        self.build_dim_products()
        self.build_dim_geolocation()
        self.build_fact_order_items()
        self.build_order_summary()
        self.build_customer_metrics()
        self.build_seller_performance()
        self.build_product_performance()

        print("\nGOLD COMPLETE")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
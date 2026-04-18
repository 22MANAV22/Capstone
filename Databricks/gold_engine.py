"""
Gold Engine — Business tables. Gold is NOT generic (business-specific logic).
"""
from pyspark.sql.functions import (
    col, count, countDistinct, sum as _sum, avg as _avg,
    max as _max, min as _min, round as _round, when, coalesce, lit, datediff
)
from table_config import S3_DELTA_SILVER, S3_DELTA_GOLD


class GoldEngine:
    """Builds Gold business tables from Silver."""

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def _read_silver(self, name):
        return self.spark.read.format("delta").load(f"{S3_DELTA_SILVER}/{name}")

    def _read_gold(self, name):
        return self.spark.read.format("delta").load(f"{S3_DELTA_GOLD}/{name}")
    def _write_gold(self, df, name):
        path = f"{S3_DELTA_GOLD}/{name}"

        self.spark.sql(f"DROP TABLE IF EXISTS gold.{name}")

        # write to your S3 path
        df.write.format("delta").mode("overwrite").save(path)

        # re-register as external table
        self.spark.sql(f"""
           CREATE TABLE gold.{name}
           USING DELTA
           LOCATION '{path}'
        """)

        c = df.count()
        self.results[name] = c
        print(f"    gold.{name} — {c:,} rows → {path}")
    

    def build_fact_order_items(self):
        print("\n  Building: fact_order_items")
        orders = self._read_silver("orders")
        items = self._read_silver("order_items")
        payments = self._read_silver("order_payments")
        reviews = self._read_silver("order_reviews")

        pay_agg = payments.groupBy("order_id").agg(
            _sum("payment_value").alias("payment_value"),
            _max("payment_type").alias("payment_type"),
            _avg("payment_installments").alias("avg_installments"))
        rev_agg = reviews.groupBy("order_id").agg(
            _avg("review_score").alias("review_score"))

        fact = items.alias("oi") \
            .join(orders.alias("o"), "order_id") \
            .join(pay_agg.alias("pa"), "order_id", "left") \
            .join(rev_agg.alias("r"), "order_id", "left") \
            .select(
                col("oi.order_id"), col("oi.order_item_id"),
                col("o.customer_id"), col("oi.product_id"), col("oi.seller_id"),
                col("oi.price"), col("oi.freight_value"),
                (col("oi.price") + col("oi.freight_value")).alias("total_amount"),
                col("pa.payment_value"), col("pa.payment_type"), col("pa.avg_installments"),
                col("o.order_status"),
                col("o.order_purchase_timestamp").alias("purchase_timestamp"),
                col("o.order_delivered_customer_date").alias("delivered_date"),
                col("o.order_estimated_delivery_date"),
                coalesce(col("r.review_score"), lit(0)).alias("review_score"),
                datediff(col("o.order_delivered_customer_date"),
                         col("o.order_purchase_timestamp")).alias("delivery_days"),
                when(col("o.order_delivered_customer_date") >
                     col("o.order_estimated_delivery_date"), 1).otherwise(0).alias("delay_flag"),
                when(col("o.order_delivered_customer_date") <=
                     col("o.order_estimated_delivery_date"), 1).otherwise(0).alias("on_time_flag"),
            )
        self._write_gold(fact, "fact_order_items")

    def build_order_summary(self):
        print("\n  Building: order_summary")
        f = self._read_gold("fact_order_items")
        df = f.groupBy("order_id").agg(
            count("order_item_id").alias("total_items"),
            _round(_sum("price"), 2).alias("total_price"),
            _round(_sum("freight_value"), 2).alias("total_freight"),
            _round(_sum("total_amount"), 2).alias("total_amount"),
            _max("order_status").alias("order_status"),
            _min("purchase_timestamp").alias("purchase_date"),
            _max("delivered_date").alias("delivered_date"),
            _round(_avg("delivery_days"), 0).cast("int").alias("delivery_days"))
        self._write_gold(df, "order_summary")

    def build_customer_metrics(self):
        print("\n  Building: customer_metrics")
        f = self._read_gold("fact_order_items")
        df = f.groupBy("customer_id").agg(
            countDistinct("order_id").alias("total_orders"),
            _round(_sum("total_amount"), 2).alias("total_spent"),
            _round(_avg("total_amount"), 2).alias("avg_order_value"),
            _max("purchase_timestamp").alias("last_order_date"),
            _round(_avg("review_score"), 1).alias("avg_review_score"))
        self._write_gold(df, "customer_metrics")

    def build_seller_performance(self):
        print("\n  Building: seller_performance")
        f = self._read_gold("fact_order_items")
        df = f.groupBy("seller_id").agg(
            countDistinct("order_id").alias("total_orders"),
            _round(_sum("price"), 2).alias("total_revenue"),
            _round(_avg("review_score"), 2).alias("avg_review_score"),
            _round((_sum("on_time_flag") * 100.0 / count("*")), 1).alias("on_time_delivery_rate"),
            _round(_avg("delivery_days"), 1).alias("avg_delivery_days"))
        self._write_gold(df, "seller_performance")

    def build_product_performance(self):
        print("\n  Building: product_performance")
        f = self._read_gold("fact_order_items")
        df = f.groupBy("product_id").agg(
            count("*").alias("total_quantity"),
            _round(_sum("price"), 2).alias("total_sales"),
            _round(_avg("price"), 2).alias("avg_price"),
            _round(_avg("review_score"), 2).alias("avg_review_score"))
        self._write_gold(df, "product_performance")
    
    def export_to_snowflake_format(self, table_name):
        print(f"\n  Exporting for Snowflake: {table_name}")

        df = self.spark.read.format("delta").load(f"{S3_DELTA_GOLD}/{table_name}")

        count = df.count()
        if count == 0:
            print(f"    ⚠ Skipping {table_name} (no data)")
            return

        export_path = f"s3://capstone-ecomm-team8/snowflake/{table_name}"

        (
           df.coalesce(1)   # 👈 important (single clean file)
             .write
             .mode("overwrite")
             .parquet(export_path)
        )

        print(f"    Exported {count:,} rows → {export_path}")

    def run(self):
        print("=" * 60)
        print("GOLD ENGINE — Building business tables")
        print("=" * 60)

        self.results = {}

        self.build_fact_order_items()
        self.export_to_snowflake_format("fact_order_items")

        self.build_order_summary()
        self.export_to_snowflake_format("order_summary")

        self.build_customer_metrics()
        self.export_to_snowflake_format("customer_metrics")

        self.build_seller_performance()
        self.export_to_snowflake_format("seller_performance")

        self.build_product_performance()
        self.export_to_snowflake_format("product_performance")

        print("\nGOLD COMPLETE")
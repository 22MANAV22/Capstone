"""
CDC Engine — Merges live stream data into Silver.
Reads new Bronze rows (batch_id='live_stream'), MERGEs into Silver.
"""
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from table_config import FACT_TABLES, S3_DELTA_BRONZE, S3_DELTA_SILVER


class CDCEngine:
    """Generic CDC MERGE engine for live stream."""

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def merge_table(self, table_name, config):
        merge_keys = config.get("merge_keys", [])
        if not merge_keys:
            print(f"    SKIP: {table_name} — no merge_keys")
            return

        print(f"\n  CDC MERGE: {table_name} (keys: {merge_keys})")

        # Read new live rows from Bronze
        bronze_path = f"{S3_DELTA_BRONZE}/{table_name}"
        try:
            df = self.spark.read.format("delta").load(bronze_path)
        except Exception as e:
            print(f"    ERROR reading Bronze: {e}")
            return

        df_live = df.filter("batch_id = 'live_stream'")
        count = df_live.count()
        if count == 0:
            print(f"    No live rows. Skipping.")
            return
        print(f"    Live rows: {count:,}")

        df_live = df_live.withColumn("cdc_merge_timestamp", current_timestamp())

        # MERGE into Silver
        silver_path = f"{S3_DELTA_SILVER}/{table_name}"
        try:
            target = DeltaTable.forPath(self.spark, silver_path)
        except:
            print(f"    Silver not found. Creating.")
            df_live.write.format("delta").mode("overwrite").save(silver_path)
            self.spark.sql(f"CREATE OR REPLACE TABLE silver.{table_name} USING DELTA LOCATION '{silver_path}'")
            self.results[table_name] = count
            return

        condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        target.alias("t").merge(df_live.alias("s"), condition) \
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        self.spark.sql(f"CREATE OR REPLACE TABLE silver.{table_name} USING DELTA LOCATION '{silver_path}'")
        final = self.spark.read.format("delta").load(silver_path).count()
        self.results[table_name] = final
        print(f"    MERGE done. silver.{table_name} = {final:,} rows")

    def run(self):
        print("=" * 60)
        print("CDC ENGINE — Merging live into Silver")
        print("=" * 60)
        self.results = {}
        for name, cfg in FACT_TABLES.items():
            self.merge_table(name, cfg)
        print(f"\nCDC COMPLETE")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
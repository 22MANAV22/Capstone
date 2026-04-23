"""
cdc_engine.py — Change Data Capture for the live stream pipeline.

When Lambda writes new CSVs to S3 live/, Bronze ingests them with
batch_id = "live_stream". This engine picks up only those rows,
applies the exact same cleaning rules that Silver uses for batch data,
and MERGEs the clean rows into Silver using Delta Lake's ACID MERGE.

Why cleaning is done here and not as a separate Silver step:
  - Silver engine does a full overwrite of reference tables — calling it
    on live data would try to rebuild customers, sellers, products from
    Bronze, but those CSVs are not in the live stream. Reference Silver
    tables would get corrupted or emptied.
  - CDC MERGE is a surgical update — only changed/new rows are touched.
  - Reusing SilverEngine._apply_rule() means the exact same cleaning
    logic applies to both batch and live paths. No duplication.

MERGE handles:
  - New orders that don't exist in Silver yet  → INSERT
  - Updated orders (e.g. status changed)       → UPDATE

FIX — DELTA_SCHEMA_NOT_PROVIDED:
  CREATE OR REPLACE TABLE fails on newer Databricks runtimes when no
  schema is provided inline and the statement is not backed by an
  AS SELECT. Replaced with CREATE TABLE IF NOT EXISTS which simply
  registers the existing Delta path in the catalog without touching
  the schema — safe to call even if the table is already registered.
"""

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from table_config import TRANSACTIONAL_TABLES, S3_DELTA_BRONZE, S3_DELTA_SILVER
from silver_engine import SilverEngine


class CDCEngine:

    def __init__(self, spark):
        self.spark   = spark
        self.results = {}
        # Borrow SilverEngine only for its _apply_rule() method.
        # We do NOT call silver.run() — that would overwrite reference tables.
        self.silver  = SilverEngine(spark)

    def _register_catalog(self, table_name, silver_path):
        """
        Register (or re-register) a Delta path in the Silver catalog.

        Uses CREATE TABLE IF NOT EXISTS instead of CREATE OR REPLACE TABLE
        to avoid DELTA_SCHEMA_NOT_PROVIDED — the latter requires an inline
        schema definition on newer Databricks runtimes when no AS SELECT
        is provided. IF NOT EXISTS is safe to call on every run: it's a
        no-op if the table is already registered, and creates the entry
        if it isn't.
        """
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS silver.{table_name} "
            f"USING DELTA LOCATION '{silver_path}'"
        )

    def merge_table(self, table_name, config):
        merge_keys = config.get("merge_keys", [])
        if not merge_keys:
            print(f"    SKIP {table_name}: no merge_keys defined in config")
            return

        print(f"\n  CDC MERGE: {table_name} (keys: {merge_keys})")

        # Read Bronze and filter to only the live stream rows.
        # batch_id = "live_stream" is set by BronzeEngine.ingest_live().
        bronze_path = f"{S3_DELTA_BRONZE}/{table_name}"
        try:
            df_bronze = self.spark.read.format("delta").load(bronze_path)
        except Exception as e:
            print(f"    ERROR reading Bronze.{table_name}: {e}")
            return

        df_live = df_bronze.filter("batch_id = 'live_stream'")
        live_count = df_live.count()

        if live_count == 0:
            print(f"    No live rows found in Bronze. Skipping.")
            return

        print(f"    Live rows before dedup: {live_count:,}")

        # ── Deduplicate BEFORE cleaning ───────────────────────────────────────
        # Delta MERGE fails with DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
        # if the source DataFrame contains more than one row with the same
        # merge key. This happens when live files accumulate in S3 without
        # archiving — Bronze live_stream partition grows across runs and
        # contains duplicate keys from repeated ingestion of the same records.
        #
        # Dedup uses dedup_keys from config if defined, falling back to
        # merge_keys — every table must be deduplicated on its merge key
        # at minimum, regardless of whether dedup_keys is explicitly set,
        # to guarantee at most one source row per target row in the MERGE.
        dedup_keys = config.get("dedup_keys") or merge_keys
        before  = df_live.count()
        df_live = df_live.dropDuplicates(dedup_keys)
        after   = df_live.count()
        if before != after:
            print(f"    Deduplication removed {before - after} duplicate rows")
        print(f"    Live rows after dedup: {after:,}")

        # ── Apply Silver cleaning rules to live rows ──────────────────────────
        # Raw live rows from Lambda go through the same type casts, null fills,
        # string trims, and value replacements that batch data gets in Silver.
        #
        # Examples of what gets fixed here:
        #   order_purchase_timestamp  → cast to proper timestamp (was a string)
        #   payment_type = "not_defined" → replaced with "unknown"
        #   payment_value             → cast to double
        #   review_score              → cast to int
        #   review_comment_title      → null filled with ""
        for rule in config.get("cleaning_rules", []):
            df_live = self.silver._apply_rule(df_live, rule)

        df_live = df_live.withColumn("cdc_merge_timestamp", current_timestamp())

        print(f"    Live rows after cleaning: {df_live.count():,}")

        # ── MERGE clean live rows into Silver ─────────────────────────────────
        silver_path = f"{S3_DELTA_SILVER}/{table_name}"

        try:
            target = DeltaTable.forPath(self.spark, silver_path)
        except Exception:
            # Silver doesn't exist yet — create it directly.
            # Handles edge case where live stream fires before any batch ran.
            print(f"    Silver.{table_name} not found. Creating from live data.")
            df_live.write.format("delta").mode("overwrite").save(silver_path)
            self._register_catalog(table_name, silver_path)
            self.results[table_name] = df_live.count()
            return

        # Build MERGE condition from merge_keys
        # e.g. orders      → "t.order_id = s.order_id"
        # e.g. order_items → "t.order_id = s.order_id AND t.order_item_id = s.order_item_id"
        condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

        (
            target.alias("t")
            .merge(df_live.alias("s"), condition)
            .whenMatchedUpdateAll()     # key exists in Silver → update all columns
            .whenNotMatchedInsertAll()  # key is new → insert full row
            .execute()
        )

        # Re-register catalog entry after MERGE — no-op if already registered
        self._register_catalog(table_name, silver_path)

        final_count = self.spark.read.format("delta").load(silver_path).count()
        self.results[table_name] = final_count
        print(f"    MERGE done. silver.{table_name} now has {final_count:,} rows")

    def run(self):
        print("=" * 60)
        print("CDC ENGINE — cleaning + merging live stream into Silver")
        print("=" * 60)
        self.results = {}

        # Only transactional tables receive live stream data.
        # Reference tables (customers, sellers, products, geolocation)
        # do not change via live stream — they stay as loaded in Batch 1.
        for name, cfg in TRANSACTIONAL_TABLES.items():
            self.merge_table(name, cfg)

        print("\nCDC COMPLETE")
        for t, c in self.results.items():
            print(f"    {t}: {c:,} rows in Silver")
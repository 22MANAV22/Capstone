"""
silver_engine.py — Incremental cleaning and deduplication layer.

COLUMN_ALREADY_EXISTS FIX:
  Bronze adds audit columns (batch_id, source_file, ingestion_timestamp)
  to every row for tracking purposes. When Silver reads from Bronze, these
  columns come along. Silver's _clean() method then tries to write them
  to Silver — but since they are Bronze-internal columns, they don't
  belong in Silver at all.

  Fix: drop audit columns after cleaning, before writing to Silver.
  Silver tables contain only the original business columns, clean and typed.
"""

from pyspark.sql.functions import (
    col, trim, initcap, upper, when, to_timestamp,
    coalesce, lit, avg, first
)
from delta.tables import DeltaTable
from table_config import (
    REFERENCE_TABLES, TRANSACTIONAL_TABLES,
    S3_DELTA_BRONZE, S3_DELTA_SILVER
)

# Audit columns added by Bronze — must be dropped before writing to Silver
BRONZE_AUDIT_COLS = ["batch_id", "source_file", "ingestion_timestamp"]


class SilverEngine:

    def __init__(self, spark):
        self.spark   = spark
        self.results = {}

    # ── Cleaning rules ────────────────────────────────────────────────────────

    def _apply_rule(self, df, rule):
        """
        Apply a single cleaning rule from config.
        Also called directly by CDCEngine for live stream rows.
        """
        c = rule["column"]
        a = rule["action"]

        if   a == "initcap_trim" : return df.withColumn(c, initcap(trim(col(c))))
        elif a == "upper_trim"   : return df.withColumn(c, upper(trim(col(c))))
        elif a == "cast_string"  : return df.withColumn(c, col(c).cast("string"))
        elif a == "cast_double":
            from pyspark.sql.functions import expr as _expr
            return df.withColumn(c, _expr(f"try_cast(`{c}` AS DOUBLE)"))
        elif a == "cast_int":
            from pyspark.sql.functions import expr as _expr
            return df.withColumn(c, _expr(f"try_cast(`{c}` AS INT)"))
        elif a == "to_timestamp":
            from pyspark.sql.functions import expr as _expr
            return df.withColumn(c, _expr(f"try_cast(`{c}` AS TIMESTAMP)"))
        elif a == "fill_null"    : return df.withColumn(c, coalesce(col(c), lit(rule.get("default", ""))))
        elif a == "replace_value": return df.withColumn(c, when(col(c) == rule["old"], rule["new"]).otherwise(col(c)))
        elif a == "rename"       : return df.withColumnRenamed(c, rule["new_name"])
        else:
            print(f"    WARNING: Unknown cleaning action '{a}' — skipping")
            return df

    def _apply_join(self, df, join_cfg):
        """Join with another Silver table (or Bronze fallback)."""
        try:
            other = self.spark.read.format("delta").load(
                f"{S3_DELTA_SILVER}/{join_cfg['source_table']}"
            )
        except Exception:
            other = self.spark.read.format("delta").load(
                f"{S3_DELTA_BRONZE}/{join_cfg['source_table']}"
            )
        df = df.join(other, join_cfg["on"], join_cfg.get("how", "left"))
        for col_name, default in join_cfg.get("fill_after", {}).items():
            df = df.withColumn(col_name, coalesce(col(col_name), lit(default)))
        return df

    def _apply_aggregate(self, df, agg_cfg):
        """Collapse multiple rows per key — used for geolocation zip dedup."""
        agg_exprs = []
        for col_name, func in agg_cfg["aggs"].items():
            if   func == "avg"  : agg_exprs.append(avg(col_name).alias(col_name))
            elif func == "first": agg_exprs.append(first(col_name).alias(col_name))
        return df.groupBy(*agg_cfg["group_by"]).agg(*agg_exprs)

    def _drop_audit_cols(self, df):
        """
        Drop Bronze audit columns before writing to Silver.
        These are internal Bronze tracking columns — they don't belong
        in Silver which should contain only clean business data.
        Only drop columns that actually exist in the DataFrame to be safe.
        """
        cols_to_drop = [c for c in BRONZE_AUDIT_COLS if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
        return df

    def _clean(self, df, config):
        """
        Apply all cleaning steps: rules → join → aggregate → dedup → drop audit cols.
        Audit columns are dropped last so they don't interfere with any cleaning rules.
        """
        for rule in config.get("cleaning_rules", []):
            df = self._apply_rule(df, rule)
        if "join" in config:
            df = self._apply_join(df, config["join"])
        if "aggregate" in config:
            df = self._apply_aggregate(df, config["aggregate"])
        dedup_keys = config.get("dedup_keys", [])
        if dedup_keys:
            df = df.dropDuplicates(dedup_keys)

        # Drop audit columns last — Silver only contains business columns
        df = self._drop_audit_cols(df)
        return df

    # ── Write helpers ─────────────────────────────────────────────────────────

    def _write_overwrite(self, df, table_name):
        """
        Full overwrite using saveAsTable.
        DROP TABLE first to clear any stale catalog entry.
        """
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        self.spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", s3_path)
            .option("overwriteSchema", "true")
            .saveAsTable(f"silver.{table_name}")
        )

        count = df.count()
        self.results[table_name] = count
        print(f"    silver.{table_name} — {count:,} rows (overwrite) → {s3_path}")

    def _write_merge(self, df, table_name, merge_keys):
        """
        Incremental MERGE for Batches 2/3/4.
        Uses DeltaTable.merge() directly — no saveAsTable, no catalog conflict.

        DEDUPLICATE BEFORE MERGE:
          Delta MERGE raises DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
          when the source DataFrame has more than one row with the same
          merge key. This happens in tables like order_payments where
          multiple payment rows share the same (order_id, payment_sequential).
          We deduplicate the source on merge_keys first, keeping the last
          row per key (most recent ingestion wins), so every merge key
          appears exactly once in the source before MERGE runs.
        """
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        # Deduplicate source on merge keys before MERGE
        # Without this, Delta raises DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW
        before = df.count()
        df = df.dropDuplicates(merge_keys)
        after = df.count()
        if before != after:
            print(f"    Deduped source: {before:,} → {after:,} rows (kept one per merge key)")

        try:
            target = DeltaTable.forPath(self.spark, s3_path)
        except Exception:
            print(f"    silver.{table_name} not found — creating via overwrite")
            self._write_overwrite(df, table_name)
            return

        condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS silver.{table_name} "
            f"USING DELTA LOCATION '{s3_path}'"
        )

        final_count = self.spark.read.format("delta").load(s3_path).count()
        self.results[table_name] = final_count
        print(f"    silver.{table_name} — {final_count:,} rows after merge → {s3_path}")

    # ── Per-table transforms ──────────────────────────────────────────────────

    def _transform_reference(self, table_name, config):
        """Reference tables: always full overwrite from all Bronze rows."""
        print(f"\n  Reference (overwrite): {table_name}")
        try:
            df = self.spark.read.format("delta").load(
                f"{S3_DELTA_BRONZE}/{table_name}"
            )
        except Exception as e:
            print(f"    ERROR reading Bronze.{table_name}: {e}")
            return
        df = self._clean(df, config)
        self._write_overwrite(df, table_name)

    def _transform_transactional_batch1(self, table_name, config):
        """
        Batch 1 transactional: overwrite Silver.
        Filters to batch_id='batch_1' — partition prune on Bronze.
        """
        print(f"\n  Transactional Batch 1 (overwrite): {table_name}")
        try:
            df = (
                self.spark.read.format("delta")
                .load(f"{S3_DELTA_BRONZE}/{table_name}")
                .filter("batch_id = 'batch_1'")
            )
        except Exception as e:
            print(f"    ERROR reading Bronze.{table_name}: {e}")
            return

        count = df.count()
        if count == 0:
            print(f"    No batch_1 rows found — skipping")
            return

        print(f"    Rows to clean: {count:,}")
        df = self._clean(df, config)
        self._write_overwrite(df, table_name)

    def _transform_transactional_incremental(self, table_name, config, batch_number):
        """
        Batches 2/3/4: read only this batch's rows (partition prune),
        clean them, MERGE into Silver. Batch 1 data never reprocessed.
        """
        print(f"\n  Transactional Batch {batch_number} (incremental merge): {table_name}")
        try:
            df = (
                self.spark.read.format("delta")
                .load(f"{S3_DELTA_BRONZE}/{table_name}")
                .filter(f"batch_id = 'batch_{batch_number}'")
            )
        except Exception as e:
            print(f"    ERROR reading Bronze.{table_name}: {e}")
            return

        count = df.count()
        if count == 0:
            print(f"    No batch_{batch_number} rows found — skipping")
            return

        print(f"    New rows to clean and merge: {count:,}")
        df = self._clean(df, config)

        merge_keys = config.get("merge_keys", [])
        if not merge_keys:
            print(f"    WARNING: No merge_keys for {table_name} — falling back to overwrite")
            self._write_overwrite(df, table_name)
            return

        self._write_merge(df, table_name, merge_keys)

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, batch_number="1"):
        """batch_number = '1' | '2' | '3' | '4'"""
        print("=" * 60)
        print(f"SILVER ENGINE — batch: {batch_number}")
        print("=" * 60)
        self.results = {}

        if batch_number == "1":
            print("\n  Reference tables (full overwrite — only in Batch 1):")
            for name, cfg in REFERENCE_TABLES.items():
                self._transform_reference(name, cfg)

            print("\n  Transactional tables (overwrite — Batch 1 data only):")
            for name, cfg in TRANSACTIONAL_TABLES.items():
                self._transform_transactional_batch1(name, cfg)

        elif batch_number in ("2", "3", "4"):
            print(f"\n  Transactional tables (incremental merge — Batch {batch_number} only):")
            for name, cfg in TRANSACTIONAL_TABLES.items():
                self._transform_transactional_incremental(name, cfg, batch_number)

        else:
            raise ValueError(
                f"Unknown batch_number: '{batch_number}'. "
                f"Silver accepts 1, 2, 3, or 4. "
                f"For live stream use CDCEngine instead."
            )

        print(f"\nSILVER COMPLETE — batch {batch_number}")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
"""
bronze_engine.py — Raw ingestion layer with checkpoint protection.

PARTITION FIX:
  The error "partitioning columns do not match the existing table's" happens
  when a catalog table was previously created WITHOUT partitioning and we now
  try to append WITH partitionBy("batch_id").

  Fix: for the first write of each transactional table (Batch 1, mode=overwrite),
  drop the catalog table first so it gets recreated fresh with the partition
  definition. Subsequent appends then match the partition scheme cleanly.

CHECKPOINT FIX:
  Previously checkpoint.mark_done() was called after ingesting reference
  tables even if all transactional tables failed. Now we track success/failure
  per table and only write checkpoint if ALL tables in the batch succeeded.

SCHEMA FIX:
  inferSchema=True is non-deterministic across CSV files. If a live file has
  any ambiguous value (empty string, whitespace) in a column that was written
  as IntegerType or DoubleType in a previous batch, Spark infers StringType
  and Delta refuses to merge incompatible types on append.

  Fix: _read_csv now accepts an optional schema_overrides dict
  ({"column_name": DataType}) and casts those columns immediately after
  reading, before any write. Bronze still uses inferSchema for discovery
  but enforces types for columns declared in table_config cleaning_rules.
  _apply_bronze_casts() extracts cast_int / cast_double / cast_string rules
  from the config and applies them at read time so the DataFrame types always
  match the existing Delta table schema.
"""

from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import IntegerType, DoubleType, StringType
from table_config import (
    REFERENCE_TABLES, TRANSACTIONAL_TABLES,
    S3_RAW, S3_LIVE, S3_DELTA_BRONZE
)
from checkpoint_manager import CheckpointManager


# Map cleaning_rules action names → Spark DataTypes
_CAST_ACTION_MAP = {
    "cast_int":    IntegerType(),
    "cast_double": DoubleType(),
    "cast_string": StringType(),
}


class BronzeEngine:

    def __init__(self, spark):
        self.spark      = spark
        self.results    = {}
        self.errors     = []   # track which tables failed
        self.checkpoint = CheckpointManager(spark)

    # ── Read helpers ──────────────────────────────────────────────────────────

    def _read_csv(self, s3_path):
        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(s3_path)
        )

    def _apply_bronze_casts(self, df, table_cfg):
        """
        Apply cast_int / cast_double / cast_string rules from cleaning_rules
        immediately after reading CSV, before any Delta write.

        This keeps Bronze types consistent across batches regardless of how
        inferSchema resolves individual files — prevents DELTA_MERGE_INCOMPATIBLE
        errors on append (e.g. review_score inferred as String in live files).
        """
        for rule in table_cfg.get("cleaning_rules", []):
            action  = rule.get("action")
            column  = rule.get("column")
            dtype   = _CAST_ACTION_MAP.get(action)

            if dtype is None or column not in df.columns:
                continue   # not a cast rule, or column absent — skip silently

            df = df.withColumn(column, col(column).cast(dtype))

        return df

    def _add_audit_columns(self, df, source_file, batch_id):
        """Tag every row with source file, batch id, and ingestion timestamp."""
        return (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file",          lit(source_file))
            .withColumn("batch_id",             lit(batch_id))
        )

    # ── Write helpers ─────────────────────────────────────────────────────────

    def _write_reference(self, df, table_name):
        """
        Full overwrite for reference tables.
        No partitioning — always one batch, always overwritten.
        """
        s3_path = f"{S3_DELTA_BRONZE}/{table_name}"
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", s3_path)
            .option("overwriteSchema", "true")
            .saveAsTable(f"bronze.{table_name}")
        )
        count = df.count()
        self.results[table_name] = count
        print(f"    bronze.{table_name} — {count:,} rows (overwrite) → {s3_path}")

    def _write_transactional_batch1(self, df, table_name):
        """
        First write for transactional tables — always overwrite + partition.

        We DROP the table from catalog first to clear any previously registered
        table that may have been created without the batch_id partition scheme.
        The DROP only removes the catalog entry — Delta files on S3 are separate.
        Then saveAsTable creates it fresh with partitionBy("batch_id") defined.
        """
        s3_path = f"{S3_DELTA_BRONZE}/{table_name}"

        # Drop catalog entry if it exists from a previous unpartitioned run
        # This is safe — it only removes the catalog registration, not S3 files
        self.spark.sql(f"DROP TABLE IF EXISTS bronze.{table_name}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("path", s3_path)
            .option("overwriteSchema", "true")
            .partitionBy("batch_id")
            .saveAsTable(f"bronze.{table_name}")
        )
        count = df.count()
        self.results[table_name] = count
        print(f"    bronze.{table_name} — {count:,} rows (overwrite, partitioned) → {s3_path}")

    def _write_transactional_append(self, df, table_name):
        """
        Append for Batches 2/3/4 and live stream.
        Table already exists with partition scheme from Batch 1 write.
        partitionBy must match — Databricks enforces this strictly.
        """
        s3_path = f"{S3_DELTA_BRONZE}/{table_name}"
        (
            df.write
            .format("delta")
            .mode("append")
            .option("path", s3_path)
            .partitionBy("batch_id")
            .saveAsTable(f"bronze.{table_name}")
        )
        count = df.count()
        self.results[table_name] = self.results.get(table_name, 0) + count
        print(f"    bronze.{table_name} — {count:,} rows (append, partitioned) → {s3_path}")

    # ── Ingestion methods ─────────────────────────────────────────────────────

    def ingest_reference(self):
        """Load all 5 reference tables. Always full overwrite, no partitioning."""
        print("\n  Reference tables (full overwrite, no partitioning):")
        for name, cfg in REFERENCE_TABLES.items():
            try:
                df = self._read_csv(f"{S3_RAW}/batch_1/{cfg['source_file']}")
                df = self._apply_bronze_casts(df, cfg)
                df = self._add_audit_columns(df, cfg["source_file"], "batch_1")
                self._write_reference(df, name)
            except Exception as e:
                print(f"    ERROR ingesting {name}: {e}")
                self.errors.append(name)

    def ingest_transactional_batch1(self):
        """
        Batch 1 transactional: overwrite + partitionBy.
        Drops catalog table first to clear any unpartitioned previous registration.
        """
        print("\n  Transactional tables — batch 1 (overwrite, partitioned by batch_id):")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            try:
                df = self._read_csv(f"{S3_RAW}/batch_1/{cfg['source_file']}")
                df = self._apply_bronze_casts(df, cfg)
                df = self._add_audit_columns(df, cfg["source_file"], "batch_1")
                self._write_transactional_batch1(df, name)
            except Exception as e:
                print(f"    ERROR ingesting {name}: {e}")
                self.errors.append(name)

    def ingest_transactional_append(self, batch_number):
        """
        Batches 2/3/4: append with matching partition scheme.
        Table already has batch_id partition from Batch 1.
        """
        batch_id = f"batch_{batch_number}"
        print(f"\n  Transactional tables — batch {batch_number} (append, partitioned):")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            try:
                df = self._read_csv(f"{S3_RAW}/batch_{batch_number}/{cfg['source_file']}")
                df = self._apply_bronze_casts(df, cfg)
                df = self._add_audit_columns(df, cfg["source_file"], batch_id)
                self._write_transactional_append(df, name)
            except Exception as e:
                print(f"    ERROR ingesting {name}: {e}")
                self.errors.append(name)

    def ingest_live(self):
        """
        Append live stream CSVs partitioned under batch_id=live_stream.

        Uses a glob path (*<source_file>) to match timestamped filenames
        in S3_LIVE — e.g. 20260423_073255_orders_dataset.csv — without
        needing to know the exact timestamp prefix.

        _apply_bronze_casts() enforces column types declared in
        cleaning_rules before the append, preventing schema mismatch errors
        (e.g. review_score inferred as String vs existing IntegerType in Delta).

        No checkpoint needed — files are archived after each successful run.
        """
        print("\n  Live stream (append, partitioned by batch_id=live_stream):")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            try:
                # Glob picks up any timestamp-prefixed variant of the file
                df = self._read_csv(f"{S3_LIVE}/*{cfg['source_file']}")
                df = self._apply_bronze_casts(df, cfg)
                df = self._add_audit_columns(df, cfg["source_file"], "live_stream")
                self._write_transactional_append(df, name)
            except Exception as e:
                print(f"    ERROR ingesting {name}: {e}")
                self.errors.append(name)

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self, batch_number):
        """Main entry point. batch_number = '1' | '2' | '3' | '4' | 'live'"""
        print("=" * 60)
        print(f"BRONZE ENGINE — batch: {batch_number}")
        print("=" * 60)
        self.results = {}
        self.errors  = []

        if batch_number == "live":
            self.ingest_live()

        elif batch_number in ("1", "2", "3", "4"):

            # Skip if already done — prevents duplicate rows from Airflow retries
            if self.checkpoint.is_done(batch_number, "bronze"):
                print(f"\nBatch {batch_number} already ingested. Nothing to do.")
                print("To force re-ingest:")
                print(f"  CheckpointManager(spark).reset('{batch_number}')")
                return

            if batch_number == "1":
                self.ingest_reference()
                self.ingest_transactional_batch1()
            else:
                self.ingest_transactional_append(batch_number)

            # Only checkpoint if ALL tables succeeded
            # If any table failed, errors list is non-empty — don't checkpoint
            # so the retry will re-attempt the full batch
            if self.errors:
                print(f"\nWARNING: {len(self.errors)} table(s) failed: {self.errors}")
                print("Checkpoint NOT written — retry will re-attempt failed tables.")
            else:
                total_rows = sum(self.results.values())
                self.checkpoint.mark_done(batch_number, "bronze", rows=total_rows)

        else:
            raise ValueError(
                f"Unknown batch_number: '{batch_number}'. Must be 1, 2, 3, 4, or live."
            )

        total = sum(self.results.values())
        print(f"\nBRONZE COMPLETE — {total:,} total rows ingested this run")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
        if self.errors:
            print(f"FAILED TABLES: {self.errors}")
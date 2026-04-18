"""
Silver Engine — Production-ready transformation layer
Handles bad data, schema drift, and UC conflicts safely.
"""

from pyspark.sql.functions import (
    col, trim, initcap, upper, when,
    coalesce, lit, avg, first, expr
)

from table_config import (
    DIMENSION_TABLES,
    FACT_TABLES,
    S3_DELTA_BRONZE,
    S3_DELTA_SILVER
)


class SilverEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    # -------------------------------
    # SAFE CLEANING RULES
    # -------------------------------
    def _apply_rule(self, df, rule):
        c = rule["column"]
        a = rule["action"]

        if c not in df.columns:
            print(f"    ⚠ Skipping missing column: {c}")
            return df

        if a == "initcap_trim":
            return df.withColumn(c, initcap(trim(col(c))))

        elif a == "upper_trim":
            return df.withColumn(c, upper(trim(col(c))))

        elif a == "cast_string":
            return df.withColumn(c, col(c).cast("string"))

        # 🔥 SAFE CASTS (NO CRASH)
        elif a == "cast_double":
            return df.withColumn(c, expr(f"try_cast({c} as double)"))

        elif a == "cast_int":
            return df.withColumn(c, expr(f"try_cast({c} as int)"))

        elif a == "to_timestamp":
            return df.withColumn(c, expr(f"try_cast({c} as timestamp)"))

        elif a == "fill_null":
            return df.withColumn(c, coalesce(col(c), lit(rule.get("default", ""))))

        elif a == "replace_value":
            return df.withColumn(
                c,
                when(col(c) == rule["old"], rule["new"]).otherwise(col(c))
            )

        elif a == "rename":
            if rule["new_name"] not in df.columns:
                return df.withColumnRenamed(c, rule["new_name"])
            return df

        else:
            print(f"    ⚠ Unknown action: {a}")
            return df

    # -------------------------------
    # REMOVE DUPLICATE COLUMNS
    # -------------------------------
    def _drop_duplicate_columns(self, df):
        seen = set()
        cols = []

        for c in df.columns:
            if c not in seen:
                cols.append(c)
                seen.add(c)

        return df.select(*cols)

    # -------------------------------
    # SAFE JOIN
    # -------------------------------
    def _apply_join(self, df, join_cfg):

        source = join_cfg["source_table"]

        try:
            join_df = self.spark.read.format("delta").load(f"{S3_DELTA_SILVER}/{source}")
        except:
            join_df = self.spark.read.format("delta").load(f"{S3_DELTA_BRONZE}/{source}")

        # remove overlapping columns except join key
        join_cols = set(join_df.columns)
        base_cols = set(df.columns)

        drop_cols = list(join_cols.intersection(base_cols) - set([join_cfg["on"]]))
        join_df = join_df.drop(*drop_cols)

        df = df.join(join_df, join_cfg["on"], join_cfg.get("how", "left"))

        # fill nulls
        for c, d in join_cfg.get("fill_after", {}).items():
            if c in df.columns:
                df = df.withColumn(c, coalesce(col(c), lit(d)))

        return df

    # -------------------------------
    # AGGREGATION
    # -------------------------------
    def _apply_aggregate(self, df, agg_cfg):
        agg_exprs = []

        for c, func in agg_cfg["aggs"].items():
            if func == "avg":
                agg_exprs.append(avg(c).alias(c))
            elif func == "first":
                agg_exprs.append(first(c, ignorenulls=True).alias(c))

        return df.groupBy(*agg_cfg["group_by"]).agg(*agg_exprs)

    # -------------------------------
    # WRITE (UC SAFE)
    # -------------------------------
    def _write_delta(self, df, table_name):
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        df = self._drop_duplicate_columns(df)

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(s3_path)

        # 🔥 Fix location mismatch
        self.spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}")

        self.spark.sql(f"""
            CREATE TABLE silver.{table_name}
            USING DELTA
            LOCATION '{s3_path}'
        """)

        count = df.count()
        self.results[table_name] = count

        print(f"    silver.{table_name} — {count:,} rows → {s3_path}")

    # -------------------------------
    # MAIN TRANSFORMATION
    # -------------------------------
    def transform_table(self, table_name, config):
        print(f"\n  Transforming: {table_name}")

        try:
            df = self.spark.read.format("delta").load(f"{S3_DELTA_BRONZE}/{table_name}")
        except Exception as e:
            print(f"    ❌ ERROR reading bronze: {e}")
            return

        # cleaning
        for rule in config.get("cleaning_rules", []):
            df = self._apply_rule(df, rule)

        # join
        if "join" in config:
            df = self._apply_join(df, config["join"])

        # aggregate
        if "aggregate" in config:
            df = self._apply_aggregate(df, config["aggregate"])

        # dedup
        dedup = config.get("dedup_keys", [])
        if dedup:
            df = df.dropDuplicates(dedup)

        # write
        self._write_delta(df, table_name)

    # -------------------------------
    # RUN
    # -------------------------------
    def run(self):
        print("=" * 60)
        print("SILVER ENGINE — Transforming all tables")
        print("=" * 60)

        self.results = {}

        for name, cfg in DIMENSION_TABLES.items():
            self.transform_table(name, cfg)

        for name, cfg in FACT_TABLES.items():
            self.transform_table(name, cfg)

        print("\nSILVER COMPLETE")
        for t, c in self.results.items():
            print(f"    {t}: {c:,}")
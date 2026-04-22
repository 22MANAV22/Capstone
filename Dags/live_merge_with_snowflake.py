"""
live_merge.py — Airflow DAG for the live stream pipeline.

Runs every 15 minutes. Lambda writes new CSVs to S3 live/ every 10 min.

Flow:
  check_live_files (ShortCircuit — skips if live/ is empty)
    → databricks_live_pipeline (bronze → cdc → gold)
    → archive_live_files
    → snowflake_load  (all 9 tables in one task)
    → notify_complete

SNS notification: one email per DAG run only.
"""

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta


class LiveMergePipeline:

    def __init__(self, bucket, db_conn, nb_root, sf_conn, sns_arn):
        self.bucket  = bucket
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn
        self.sns_arn = sns_arn

    # ── SNS — one notification per DAG run ───────────────────────────────────

    def _publish_sns(self, subject, message):
        """
        Pull credentials from aws_default Airflow connection explicitly.
        boto3 alone cannot find them automatically.
        """
        try:
            hook  = S3Hook(aws_conn_id="aws_default")
            creds = hook.get_credentials()
            sns   = boto3.client(
                "sns",
                region_name="us-east-1",
                aws_access_key_id=creds.access_key,
                aws_secret_access_key=creds.secret_key,
            )
            sns.publish(
                TopicArn=self.sns_arn,
                Subject=subject[:100],
                Message=message
            )
            print(f"SNS sent: {subject}")
        except Exception as e:
            print(f"SNS skipped: {e}")

    def on_failure(self, context):
        """Called once when any task fails — sent via default_args."""
        task = context["task_instance"].task_id
        self._publish_sns(
            f"FAILURE: live pipeline — {task}",
            f"Live pipeline task '{task}' FAILED at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n"
            f"DAG run: {context['dag_run'].run_id}\n"
            f"Check Airflow logs for details."
        )

    def notify_complete(self, **context):
        """Called once at the very end of a successful DAG run."""
        self._publish_sns(
            "Live pipeline complete",
            f"Live pipeline completed successfully at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\n"
            f"Bronze, CDC, Gold and Snowflake all updated."
        )

    # ── Task callables ────────────────────────────────────────────────────────

    def check_live_files(self, **ctx):
        """
        ShortCircuit: skip entire DAG if live/ has no real CSV files.
        Filters out the folder key itself (ends with /) to avoid false positives.
        """
        hook  = S3Hook(aws_conn_id="aws_default")
        keys  = hook.list_keys(bucket_name=self.bucket, prefix="live/") or []
        files = [k for k in keys if not k.endswith("/")]
        print(f"Live files found: {len(files)}")
        return len(files) > 0

    def archive_live_files(self, **ctx):
        """
        Move processed live files from live/ to live_archive/.
        Called before Snowflake load — data is safely in Gold on S3.
        If Snowflake fails, re-run just the Snowflake task.
        """
        hook  = S3Hook(aws_conn_id="aws_default")
        keys  = hook.list_keys(bucket_name=self.bucket, prefix="live/") or []
        files = [k for k in keys if not k.endswith("/")]

        if not files:
            print("No live files to archive")
            return

        for key in files:
            dest = key.replace("live/", "live_archive/", 1)
            hook.copy_object(
                source_bucket_name=self.bucket,
                source_bucket_key=key,
                dest_bucket_name=self.bucket,
                dest_bucket_key=dest,
            )
            hook.delete_objects(bucket=self.bucket, keys=[key])
            print(f"Archived: {key} → {dest}")

    # ── Snowflake — single task, all 9 tables ─────────────────────────────────

    def snowflake_load(self):
        """
        Load all 9 Gold tables into Snowflake in one task.
        Order: dims → fact → aggregates (respects star schema dependencies).
        Each table: TRUNCATE first, then COPY INTO.
        FORCE = TRUE ensures re-runs always reload even if files unchanged.
        """
        tables_in_order = [
            "dim_customers",
            "dim_sellers",
            "dim_products",
            "dim_geolocation",
            "fact_order_items",
            "order_summary",
            "customer_metrics",
            "seller_performance",
            "product_performance",
        ]

        sql_statements = []
        for table in tables_in_order:
            sql_statements.append(f"TRUNCATE TABLE {table};")
            sql_statements.append(f"""
                COPY INTO {table}
                FROM @gold_s3_stage/{table}/
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                FORCE = TRUE
                ON_ERROR = 'CONTINUE';
            """)

        return SQLExecuteQueryOperator(
            task_id="snowflake_load",
            conn_id=self.sf_conn,
            sql=sql_statements,
        )

    # ── DAG ───────────────────────────────────────────────────────────────────

    def build_dag(self):
        with DAG(
            dag_id="live_merge",
            default_args={
                "owner"              : "capstone-team8",
                "retries"            : 1,
                "retry_delay"        : timedelta(minutes=2),
                "on_failure_callback": self.on_failure,
            },
            schedule="*/15 * * * *",
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=["capstone", "live"],
        ) as dag:

            check = ShortCircuitOperator(
                task_id="check_live_files",
                python_callable=self.check_live_files,
            )

            databricks = DatabricksSubmitRunOperator(
                task_id="databricks_live_pipeline",
                databricks_conn_id=self.db_conn,
                run_name="live_merge_pipeline",
                tasks=[
                    {
                        "task_key"     : "bronze_live",
                        "notebook_task": {
                            "notebook_path"  : f"{self.nb_root}/run_bronze",
                            "base_parameters": {"batch_number": "live"},
                        },
                    },
                    {
                        "task_key"   : "cdc_merge",
                        "depends_on" : [{"task_key": "bronze_live"}],
                        "notebook_task": {
                            "notebook_path": f"{self.nb_root}/run_cdc",
                        },
                    },
                    {
                        "task_key"   : "gold_rebuild",
                        "depends_on" : [{"task_key": "cdc_merge"}],
                        "notebook_task": {
                            "notebook_path": f"{self.nb_root}/run_gold",
                        },
                    },
                ],
            )

            archive = PythonOperator(
                task_id="archive_live_files",
                python_callable=self.archive_live_files,
            )

            sf_load = self.snowflake_load()

            notify = PythonOperator(
                task_id="notify_complete",
                python_callable=self.notify_complete,
            )

            # ── Wire ──────────────────────────────────────────────────────────
            check >> databricks >> archive >> sf_load >> notify

        return dag


# ── Instantiate ───────────────────────────────────────────────────────────────
live = LiveMergePipeline(
    bucket  = "capstone-ecomm-team8",
    db_conn = "databricks_default",
    nb_root = "/Shared/Capstone",
    sf_conn = "snowflake_default",
    sns_arn = "arn:aws:sns:us-east-1:868859238853:Capstone-team8",
)
dag = live.build_dag()
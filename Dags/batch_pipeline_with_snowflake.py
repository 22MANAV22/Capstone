"""
batch_pipeline.py — Airflow DAG for batch processing.

Triggered manually 3 times (for batches 2, 3, 4) after Batch 1 is
tested manually in Databricks.

Full flow per trigger:
  move_staging_to_raw
    → sense_raw_files
    → run_bronze
    → run_silver
    → run_gold
    → snowflake_load  (all 9 tables in one task)
    → notify_complete

SNS notification: one email per DAG run only (success at end, failure on any task).

How to trigger from EC2:
  airflow dags trigger batch_pipeline --conf '{"batch_number": "2"}'
  airflow dags trigger batch_pipeline --conf '{"batch_number": "3"}'
  airflow dags trigger batch_pipeline --conf '{"batch_number": "4"}'
"""

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta


class BatchPipeline:

    def __init__(self, bucket, sns_arn, db_conn, nb_root, sf_conn):
        self.bucket  = bucket
        self.sns_arn = sns_arn
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn

    # ── SNS — one notification per DAG run ───────────────────────────────────

    def _publish_sns(self, subject, message):
        """
        Pull credentials from aws_default Airflow connection explicitly.
        boto3 alone cannot find them — Airflow does not inject them as
        environment variables automatically.
        """
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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
        task  = context["task_instance"].task_id
        batch = context["dag_run"].conf.get("batch_number", "?")
        self._publish_sns(
            f"FAILURE: batch_{batch} — {task}",
            f"Task '{task}' FAILED for batch {batch}.\n"
            f"DAG run: {context['dag_run'].run_id}\n"
            f"Check Airflow logs for details."
        )

    def notify_complete(self, **context):
        """Called once at the very end of a successful DAG run."""
        batch = context["dag_run"].conf.get("batch_number", "?")
        self._publish_sns(
            f"Batch {batch} complete",
            f"Full pipeline for batch {batch} finished successfully "
            f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\n"
            f"All Bronze, Silver, Gold and Snowflake tasks completed."
        )

    # ── Task callables ────────────────────────────────────────────────────────

    def move_staging_to_raw(self, **context):
        batch = context["dag_run"].conf.get("batch_number", "2")
        src   = f"staging/batch_{batch}/"
        dst   = f"raw/batch_{batch}/"

        hook       = S3Hook(aws_conn_id="aws_default")
        keys       = hook.list_keys(bucket_name=self.bucket, prefix=src) or []
        real_files = [k for k in keys if not k.endswith("/")]

        if not real_files:
            print(f"No files found in {src} — nothing to move")
            return

        for key in real_files:
            dest_key = f"{dst}{key.split('/')[-1]}"
            hook.copy_object(
                source_bucket_name=self.bucket,
                source_bucket_key=key,
                dest_bucket_name=self.bucket,
                dest_bucket_key=dest_key,
            )
            hook.delete_objects(bucket=self.bucket, keys=[key])
            print(f"Moved: {key} → {dest_key}")

    # ── Databricks task factory ───────────────────────────────────────────────

    def _db_task(self, task_id, notebook, extra_params=None):
        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id=self.db_conn,
            run_name=f"batch_pipeline_{task_id}",
            tasks=[{
                "task_key"     : task_id,
                "notebook_task": {
                    "notebook_path"  : f"{self.nb_root}/{notebook}",
                    "base_parameters": extra_params or {},
                },
            }],
        )

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
            dag_id="batch_pipeline",
            default_args={
                "owner"              : "capstone-team8",
                "retries"            : 2,
                "retry_delay"        : timedelta(minutes=3),
                "on_failure_callback": self.on_failure,
            },
            schedule=None,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=["capstone", "batch"],
        ) as dag:

            move = PythonOperator(
                task_id="move_staging_to_raw",
                python_callable=self.move_staging_to_raw,
            )

            sense = S3KeySensor(
                task_id="sense_raw_files",
                bucket_name=self.bucket,
                bucket_key='raw/batch_{{ dag_run.conf.get("batch_number", "2") }}/*',
                wildcard_match=True,
                aws_conn_id="aws_default",
                timeout=300,
                poke_interval=15,
            )

            bronze = self._db_task(
                "run_bronze", "run_bronze",
                extra_params={
                    "batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'
                }
            )
            silver = self._db_task(
                "run_silver", "run_silver",
                extra_params={
                    "batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'
                }
            )
            gold = self._db_task("run_gold", "run_gold")

            sf_load = self.snowflake_load()

            notify = PythonOperator(
                task_id="notify_complete",
                python_callable=self.notify_complete,
            )

            # ── Wire ──────────────────────────────────────────────────────────
            move >> sense >> bronze >> silver >> gold >> sf_load >> notify

        return dag


# ── Instantiate ───────────────────────────────────────────────────────────────
pipeline = BatchPipeline(
    bucket  = "capstone-ecomm-team8",
    sns_arn = "arn:aws:sns:us-east-1:868859238853:Capstone-team8",
    db_conn = "databricks_default",
    nb_root = "/Shared/Capstone",
    sf_conn = "snowflake_default",
)
dag = pipeline.build_dag()
"""
Live Merge — runs every 15 min.
check → bronze(live) → cdc → gold → snowflake_load → archive

Bronze is NOT skipped. Archive moves live/ → live_archive/.
Data is loaded to Snowflake after each live merge.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3


class LiveMergePipeline:

    LIVE_FILES = [
        'live/orders_dataset.csv',
        'live/order_items_dataset.csv',
        'live/order_payments_dataset.csv',
        'live/order_reviews_dataset.csv',
    ]

    def __init__(self, bucket, db_conn, nb_root, sf_conn):
        self.bucket = bucket
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn

    def check_live(self, **ctx):
        s3 = boto3.client('s3', region_name='us-east-1')
        r = s3.list_objects_v2(Bucket=self.bucket, Prefix='live/', MaxKeys=5)
        found = 'Contents' in r and len(r['Contents']) > 0
        print(f"Live files found: {found}")
        return found

    def archive(self, **ctx):
        s3 = boto3.client('s3', region_name='us-east-1')
        for k in self.LIVE_FILES:
            try:
                s3.copy_object(Bucket=self.bucket,
                    CopySource={'Bucket': self.bucket, 'Key': k},
                    Key=k.replace('live/', 'live_archive/'))
                s3.delete_object(Bucket=self.bucket, Key=k)
                print(f"Archived: {k}")
            except Exception as e:
                print(f"Skip {k}: {e}")

    def build_dag(self):
        with DAG(
            'live_merge',
            default_args={'owner': 'capstone-team', 'retries': 1,
                          'retry_delay': timedelta(minutes=2)},
            description='Live: check→bronze→cdc→gold→snowflake→archive',
            schedule_interval='*/15 * * * *',  # Every 15 minutes
            start_date=days_ago(1),
            catchup=False,
            tags=['capstone', 'live'],
            paused=True,  # ← START PAUSED! Enable after batch 4 completes
        ) as dag:

            check = ShortCircuitOperator(
                task_id='check_live_files',
                python_callable=self.check_live)

            bronze_live = DatabricksSubmitRunOperator(
                task_id='bronze_ingest_live',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_bronze",
                    "base_parameters": {"batch_number": "live"}})

            cdc = DatabricksSubmitRunOperator(
                task_id='cdc_merge_to_silver',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_cdc"})

            gold = DatabricksSubmitRunOperator(
                task_id='rebuild_gold',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_gold"})

            # ✅ NEW: Snowflake COPY INTO for live data
            copy_live_fact_order_items = SnowflakeOperator(
                task_id='copy_live_fact_order_items',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO fact_order_items
                    FROM @gold_s3_stage/fact_order_items/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """)

            copy_live_order_summary = SnowflakeOperator(
                task_id='copy_live_order_summary',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO order_summary
                    FROM @gold_s3_stage/order_summary/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """)

            copy_live_customer_metrics = SnowflakeOperator(
                task_id='copy_live_customer_metrics',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO customer_metrics
                    FROM @gold_s3_stage/customer_metrics/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """)

            copy_live_seller_performance = SnowflakeOperator(
                task_id='copy_live_seller_performance',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO seller_performance
                    FROM @gold_s3_stage/seller_performance/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """)

            copy_live_product_performance = SnowflakeOperator(
                task_id='copy_live_product_performance',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO product_performance
                    FROM @gold_s3_stage/product_performance/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """)

            arch = PythonOperator(
                task_id='archive_live_files',
                python_callable=self.archive)

            # ✅ NEW: Pipeline includes Snowflake loads
            check >> bronze_live >> cdc >> gold >> [
                copy_live_fact_order_items,
                copy_live_order_summary,
                copy_live_customer_metrics,
                copy_live_seller_performance,
                copy_live_product_performance
            ] >> arch

        return dag

# ✅ UPDATE THESE WITH YOUR VALUES:
p = LiveMergePipeline(
    bucket="capstone-ecomm-team8",  # ← Your bucket name
    db_conn="databricks_default",
    nb_root="/Shared/capstone",
    sf_conn="snowflake_default")  # ← Snowflake connection

dag = p.build_dag()

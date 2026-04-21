"""
Live Merge Pipeline — Airflow 3 compatible
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import boto3


class LiveMergePipeline:

    def __init__(self, bucket, db_conn, nb_root, sf_conn):
        self.bucket = bucket
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn

    def check_live(self, **ctx):
        s3 = boto3.client('s3')
        r = s3.list_objects_v2(Bucket=self.bucket, Prefix='live/')
        return 'Contents' in r

    def archive(self, **ctx):
        s3 = boto3.client('s3')
        for obj in s3.list_objects_v2(Bucket=self.bucket, Prefix='live/').get('Contents', []):
            k = obj['Key']
            s3.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': k},
                Key=k.replace('live/', 'live_archive/')
            )
            s3.delete_object(Bucket=self.bucket, Key=k)

    def build_dag(self):
        with DAG(
            dag_id='live_merge',
            schedule="*/15 * * * *",
            start_date=datetime(2024, 1, 1),
            catchup=False,
        ) as dag:

            check = ShortCircuitOperator(
                task_id='check_live',
                python_callable=self.check_live
            )

            bronze = DatabricksSubmitRunOperator(
                task_id='bronze',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_bronze"},
            )

            cdc = DatabricksSubmitRunOperator(
                task_id='cdc',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_cdc"},
            )

            gold = DatabricksSubmitRunOperator(
                task_id='gold',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_gold"},
            )

            def sf_task(task_id, table):
                return SQLExecuteQueryOperator(
                    task_id=task_id,
                    conn_id=self.sf_conn,
                    sql=f"""

                        TRUNCATE TABLE {table};


                        COPY INTO {table}
                        FROM @gold_s3_stage/{table}/
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                        ON_ERROR = 'CONTINUE';
                    """
                )

            sf_tasks = [
                sf_task("copy_live_fact_order_items", "fact_order_items"),
                sf_task("copy_live_order_summary", "order_summary"),
                sf_task("copy_live_customer_metrics", "customer_metrics"),
                sf_task("copy_live_seller_performance", "seller_performance"),
                sf_task("copy_live_product_performance", "product_performance"),
            ]

            archive = PythonOperator(
                task_id='archive',
                python_callable=self.archive
            )

            check >> bronze >> cdc >> gold >> sf_tasks >> archive

        return dag


live_pipeline = LiveMergePipeline(
    bucket="capstone-ecomm-team8",
    db_conn="databricks_default",
    nb_root="/Shared/capstone",
    sf_conn="snowflake_default"
)

dag = live_pipeline.build_dag()
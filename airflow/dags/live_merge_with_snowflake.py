from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import boto3


class LiveMergePipeline:

    def __init__(self, bucket, db_conn, nb_root, sf_conn, sns_arn):
        self.bucket = bucket
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn
        self.sns_arn = sns_arn

    # -------------------------
    # SNS
    # -------------------------
    def send_sns(self, subject, message):
        try:
            boto3.client('sns', region_name='us-east-1').publish(
                TopicArn=self.sns_arn,
                Subject=subject[:100],
                Message=message
            )
        except Exception as e:
            print(f"SNS failed: {e}")

    def on_failure(self, context):
        task = context['task_instance'].task_id
        self.send_sns(
            f"FAILURE: live/{task}",
            f"{task} FAILED at {datetime.now()}"
        )

    def notify_done(self, **context):
        self.send_sns(
            "LIVE PIPELINE SUCCESS",
            f"Live pipeline completed at {datetime.now()}"
        )

    # -------------------------
    # CHECK LIVE FILES
    # -------------------------
    def check_live(self, **ctx):
        s3 = boto3.client('s3', region_name='us-east-1')
        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix='live/', MaxKeys=5)

        found = 'Contents' in resp and len(resp['Contents']) > 0
        print(f"Live files present: {found}")
        return found

    # -------------------------
    # ARCHIVE
    # -------------------------
    def archive(self, **ctx):
        s3 = boto3.client('s3', region_name='us-east-1')

        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix='live/')
        if 'Contents' not in resp:
            print("No live files to archive")
            return

        for obj in resp['Contents']:
            key = obj['Key']

            if key.endswith('/'):
                continue

            dest = key.replace('live/', 'live_archive/')

            s3.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': key},
                Key=dest
            )
            s3.delete_object(Bucket=self.bucket, Key=key)

            print(f"Archived: {key} → {dest}")

    # -------------------------
    # DAG
    # -------------------------
    def build_dag(self):
        with DAG(
            dag_id='live_merge',
            default_args={
                'owner': 'capstone-team',
                'retries': 1,
                'retry_delay': timedelta(minutes=2),
                'on_failure_callback': self.on_failure,
            },
            schedule="*/15 * * * *",
            start_date=datetime(2024, 1, 1),
            catchup=False,
        ) as dag:

            check = ShortCircuitOperator(
                task_id='check_live_files',
                python_callable=self.check_live
            )

            bronze = DatabricksSubmitRunOperator(
                task_id='run_bronze_live',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_bronze",
                    "base_parameters": {
                        "batch_number": "live"
                    },
                },
            )

            cdc = DatabricksSubmitRunOperator(
                task_id='run_cdc',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_cdc"
                },
            )

            gold = DatabricksSubmitRunOperator(
                task_id='run_gold',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_gold"
                },
            )

            # Snowflake full refresh
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
                sf_task("copy_fact_order_items", "fact_order_items"),
                sf_task("copy_order_summary", "order_summary"),
                sf_task("copy_customer_metrics", "customer_metrics"),
                sf_task("copy_seller_performance", "seller_performance"),
                sf_task("copy_product_performance", "product_performance"),
            ]

            archive = PythonOperator(
                task_id='archive_live_files',
                python_callable=self.archive
            )

            notify = PythonOperator(
                task_id='notify_complete',
                python_callable=self.notify_done
            )

            check >> bronze >> cdc >> gold >> sf_tasks >> archive >> notify

        return dag


live_pipeline = LiveMergePipeline(
    bucket="capstone-ecomm-team8",
    db_conn="databricks_default",
    nb_root="/Shared/Capstone",
    sf_conn="snowflake_default",
    sns_arn="arn:aws:sns:us-east-1:868859238853:Capstone-team8"
)

dag = live_pipeline.build_dag()
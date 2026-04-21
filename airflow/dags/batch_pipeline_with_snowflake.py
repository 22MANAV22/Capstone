from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import boto3


class BatchPipeline:

    def __init__(self, bucket, sns_arn, db_conn, nb_root, sf_conn):
        self.bucket = bucket
        self.sns_arn = sns_arn
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn

    # -------------------------
    # ALERTS
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

    def on_success(self, context):
        task = context['task_instance'].task_id
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(
            f"SUCCESS: batch_{batch}/{task}",
            f"{task} completed for batch {batch}"
        )

    def on_failure(self, context):
        task = context['task_instance'].task_id
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(
            f"FAILURE: batch_{batch}/{task}",
            f"{task} FAILED for batch {batch}"
        )

    # -------------------------
    # S3 MOVE
    # -------------------------
    def move_s3(self, **context):
        batch = context['dag_run'].conf.get('batch_number', '2')
        src = f"staging/batch_{batch}/"
        dst = f"raw/batch_{batch}/"

        s3 = boto3.client('s3', region_name='us-east-1')
        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix=src)

        if 'Contents' not in resp:
            print(f"No files in {src}")
            return

        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue

            dest = f"{dst}{key.split('/')[-1]}"

            s3.copy_object(
                Bucket=self.bucket,
                CopySource={'Bucket': self.bucket, 'Key': key},
                Key=dest
            )
            s3.delete_object(Bucket=self.bucket, Key=key)

            print(f"Moved: {key} → {dest}")

    def notify_done(self, **context):
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(
            f"BATCH {batch} COMPLETE",
            f"Batch {batch} finished successfully at {datetime.now()}"
        )

    # -------------------------
    # DAG
    # -------------------------
    def build_dag(self):
        with DAG(
            dag_id='batch_pipeline',
            default_args={
                'owner': 'capstone-team',
                'retries': 2,
                'retry_delay': timedelta(minutes=3),
                'on_failure_callback': self.on_failure,
            },
            schedule=None,
            start_date=datetime(2024, 1, 1),
            catchup=False,
        ) as dag:

            move = PythonOperator(
                task_id='move_staging_to_raw',
                python_callable=self.move_s3
            )

            sense = S3KeySensor(
                task_id='sense_files',
                bucket_name=self.bucket,
                bucket_key='raw/batch_{{ dag_run.conf.get("batch_number", "2") }}/*',
                wildcard_match=True,
                aws_conn_id='aws_default',
                timeout=300,
                poke_interval=15
            )

            bronze = DatabricksSubmitRunOperator(
                task_id='run_bronze',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_bronze",
                    "base_parameters": {
                        "batch_number": '{{ dag_run.conf.get("batch_number","2") }}'
                    },
                },
                on_success_callback=self.on_success
            )

            silver = DatabricksSubmitRunOperator(
                task_id='run_silver',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_silver"
                },
                on_success_callback=self.on_success
            )

            gold = DatabricksSubmitRunOperator(
                task_id='run_gold',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_gold"
                },
                on_success_callback=self.on_success
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
                sf_task("copy_fact_order_items", "fact_order_items"),
                sf_task("copy_order_summary", "order_summary"),
                sf_task("copy_customer_metrics", "customer_metrics"),
                sf_task("copy_seller_performance", "seller_performance"),
                sf_task("copy_product_performance", "product_performance"),
            ]

            notify = PythonOperator(
                task_id='notify_complete',
                python_callable=self.notify_done
            )

            move >> sense >> bronze >> silver >> gold >> sf_tasks >> notify

        return dag


batch_pipeline = BatchPipeline(
    bucket="capstone-ecomm-team8",
    sns_arn="arn:aws:sns:us-east-1:868859238853:Capstone-team8",
    db_conn="databricks_default",
    nb_root="/Shared/Capstone",
    sf_conn="snowflake_default"
)

dag = batch_pipeline.build_dag()
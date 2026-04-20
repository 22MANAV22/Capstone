"""
Batch Pipeline — ONE parameterized DAG, triggered 3 times.

Batch 1: Manual in Databricks (NOT here).
Batch 2: airflow dags trigger batch_pipeline --conf '{"batch_number":"2"}'
Batch 3: airflow dags trigger batch_pipeline --conf '{"batch_number":"3"}'
Batch 4: airflow dags trigger batch_pipeline --conf '{"batch_number":"4"}'

Each trigger: move → sense → bronze → silver → gold → snowflake_load → notify
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import boto3


class BatchPipeline:

    def __init__(self, bucket, sns_arn, db_conn, nb_root, sf_conn):
        self.bucket = bucket
        self.sns_arn = sns_arn
        self.db_conn = db_conn
        self.nb_root = nb_root
        self.sf_conn = sf_conn

    def send_sns(self, subject, message):
        try:
            boto3.client('sns', region_name='us-east-1').publish(
                TopicArn=self.sns_arn, Subject=subject[:100], Message=message)
        except Exception as e:
            print(f"SNS failed: {e}")

    def on_success(self, context):
        task = context['task_instance'].task_id
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(f"SUCCESS: batch_{batch}/{task}",
                      f"Batch {batch}, {task} done at {datetime.now()}")

    def on_failure(self, context):
        task = context['task_instance'].task_id
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(f"FAILURE: batch_{batch}/{task}",
                      f"Batch {batch}, {task} FAILED: {context.get('exception','?')}")

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
            k = obj['Key']
            if k.endswith('/'):
                continue
            d = f"{dst}{k.split('/')[-1]}"
            s3.copy_object(Bucket=self.bucket, CopySource={'Bucket': self.bucket, 'Key': k}, Key=d)
            s3.delete_object(Bucket=self.bucket, Key=k)
            print(f"Moved: {k} → {d}")

    def notify_done(self, **context):
        batch = context['dag_run'].conf.get('batch_number', '?')
        self.send_sns(f"BATCH {batch} COMPLETE",
                      f"Batch {batch}: Bronze→Silver→Gold→Snowflake done.\n"
                      f"Check Snowflake warehouse for data.\nTime: {datetime.now()}")

    def build_dag(self):
        with DAG(
            'batch_pipeline',
            default_args={
                'owner': 'capstone-team', 'retries': 2,
                'retry_delay': timedelta(minutes=3),
                'on_failure_callback': self.on_failure,
            },
            description='Batch: move→bronze→silver→gold→snowflake (one batch per trigger)',
            schedule_interval=None,
            start_date=days_ago(1),
            catchup=False,
            tags=['capstone', 'batch'],
        ) as dag:

            move = PythonOperator(
                task_id='move_staging_to_raw',
                python_callable=self.move_s3,
                provide_context=True)

            sense = S3KeySensor(
                task_id='sense_files',
                bucket_name=self.bucket,
                bucket_key='raw/batch_{{ dag_run.conf.get("batch_number", "2") }}/',
                wildcard_match=True,
                aws_conn_id='aws_default',
                timeout=300, poke_interval=15)

            bronze = DatabricksSubmitRunOperator(
                task_id='run_bronze',
                databricks_conn_id=self.db_conn,
                notebook_task={
                    "notebook_path": f"{self.nb_root}/run_bronze",
                    "base_parameters": {"batch_number": '{{ dag_run.conf.get("batch_number","2") }}'},
                },
                on_success_callback=self.on_success)

            silver = DatabricksSubmitRunOperator(
                task_id='run_silver',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_silver"},
                on_success_callback=self.on_success)

            gold = DatabricksSubmitRunOperator(
                task_id='run_gold',
                databricks_conn_id=self.db_conn,
                notebook_task={"notebook_path": f"{self.nb_root}/run_gold"},
                on_success_callback=self.on_success)

            # ✅ NEW: Snowflake COPY INTO tasks
            copy_fact_order_items = SnowflakeOperator(
                task_id='copy_fact_order_items',
                snowflake_conn_id=self.sf_conn,
                sql=f"""
                    COPY INTO fact_order_items
                    FROM @gold_s3_stage/fact_order_items/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """,
                on_success_callback=self.on_success)

            copy_order_summary = SnowflakeOperator(
                task_id='copy_order_summary',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO order_summary
                    FROM @gold_s3_stage/order_summary/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """,
                on_success_callback=self.on_success)

            copy_customer_metrics = SnowflakeOperator(
                task_id='copy_customer_metrics',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO customer_metrics
                    FROM @gold_s3_stage/customer_metrics/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """,
                on_success_callback=self.on_success)

            copy_seller_performance = SnowflakeOperator(
                task_id='copy_seller_performance',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO seller_performance
                    FROM @gold_s3_stage/seller_performance/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """,
                on_success_callback=self.on_success)

            copy_product_performance = SnowflakeOperator(
                task_id='copy_product_performance',
                snowflake_conn_id=self.sf_conn,
                sql="""
                    COPY INTO product_performance
                    FROM @gold_s3_stage/product_performance/
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = 'CONTINUE';
                """,
                on_success_callback=self.on_success)

            notify = PythonOperator(
                task_id='notify_complete',
                python_callable=self.notify_done,
                provide_context=True)

            # ✅ NEW: Pipeline includes Snowflake loads
            move >> sense >> bronze >> silver >> gold >> [
                copy_fact_order_items,
                copy_order_summary,
                copy_customer_metrics,
                copy_seller_performance,
                copy_product_performance
            ] >> notify

        return dag

# ✅ UPDATE THESE WITH YOUR VALUES:
p = BatchPipeline(
    bucket="capstone-ecomm-team8",  # ← Your bucket name
    sns_arn="arn:aws:sns:us-east-1:YOUR_ACCOUNT:capstone-pipeline-alerts",
    db_conn="databricks_default",
    nb_root="/Shared/capstone",
    sf_conn="snowflake_default")  # ← Snowflake connection

dag = p.build_dag()

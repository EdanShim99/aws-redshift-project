from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import requests
import boto3
import json

S3_BUCKET = "ecommerce-lakehousev2"
S3_PREFIX = "bronze/users/"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

def extract_and_upload_to_s3():
    response = requests.get("https://dummyjson.com/users?limit=100")
    response.raise_for_status()

    users = response.json()["users"]

    s3 = boto3.client("s3")

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{S3_PREFIX}users_{timestamp}.json"

    json_lines = "\n".join(json.dumps(user) for user in users)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=filename,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Uploaded to s3://{S3_BUCKET}/{filename}")


with DAG(
    dag_id="ecommerce_medallion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["redshift", "medallion"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_dummyjson_to_s3",
        python_callable=extract_and_upload_to_s3,
    )

    create_bronze_schema = SQLExecuteQueryOperator(
        task_id="create_bronze_schema",
        conn_id="redshift_conn",
        sql="sql/bronze/create_bronze_schema.sql",
    )

    create_bronze_table = SQLExecuteQueryOperator(
        task_id="create_bronze_table",
        conn_id="redshift_conn",
        sql="sql/bronze/create_bronze_table.sql",
    )

    load_bronze = SQLExecuteQueryOperator(
        task_id="copy_json_to_bronze",
        conn_id="redshift_conn",
        sql="sql/bronze/copy_to_bronze.sql",
    )

    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="redshift_conn",
        sql="sql/silver/create_silver_schema.sql",
    )

    create_silver_table = SQLExecuteQueryOperator(
        task_id="create_silver_table",
        conn_id="redshift_conn",
        sql="sql/silver/create_silver_table.sql",
    )
    
    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="redshift_conn",
        sql="sql/silver/transform_silver.sql",
    )

    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="redshift_conn",
        sql="sql/gold/create_gold_schema.sql",
    )

    create_gold_table = SQLExecuteQueryOperator(
        task_id="create_gold_table",
        conn_id="redshift_conn",
        sql="sql/gold/create_gold_table.sql",
    )

    transform_gold = SQLExecuteQueryOperator(
        task_id="transform_to_gold",
        conn_id="redshift_conn",
        sql="sql/gold/transform_gold.sql",
    )

    (
    extract_task
    >> create_bronze_schema
    >> create_bronze_table
    >> load_bronze
    >> create_silver_schema
    >> create_silver_table
    >> transform_silver
    >> create_gold_schema
    >> create_gold_table
    >> transform_gold
)   
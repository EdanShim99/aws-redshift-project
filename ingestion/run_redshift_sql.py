import psycopg2
import os


REDSHIFT_HOST = "ecommerce-workgroup.590807097292.us-west-1.redshift-serverless.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PORT = 5439
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")


def run_sql_file(filepath):
    print(f"Running SQL file: {filepath}")

    with open(filepath, "r") as f:
        sql = f.read()

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        port=REDSHIFT_PORT
    )

    conn.autocommit = True
    cursor = conn.cursor()

    # Split SQL statements by semicolon
    statements = sql.split(";")

    for statement in statements:
        if statement.strip():
            cursor.execute(statement)

    cursor.close()
    conn.close()

    print(f"Finished executing {filepath}")


def create_bronze():
    run_sql_file("sql/create_bronze.sql")


def copy_to_bronze():
    run_sql_file("sql/copy_to_bronze.sql")


def transform_silver():
    run_sql_file("sql/silver_transform.sql")


def transform_gold():
    run_sql_file("sql/gold_transform.sql")
    
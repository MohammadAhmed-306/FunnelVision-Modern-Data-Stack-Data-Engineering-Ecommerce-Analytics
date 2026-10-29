from pathlib import Path
from datetime import timedelta

import pendulum

from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DATA_DIR = Path("/opt/airflow/data/raw")

DBT_PROJECT_DIR = "/opt/airflow/dbt/funnelvision"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

SNOWFLAKE_DATABASE = "FUNNELVISION_DB"
SNOWFLAKE_SCHEMA = "RAW"
SNOWFLAKE_CONN_ID = "snowflake_default"

# Named internal stage used to bulk-load the CSV files.
SNOWFLAKE_STAGE = (
    f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.FUNNELVISION_LOAD_STAGE"
)

# CSV files have a header row. Values are mapped by the explicit column
# list below, so the order of columns in each CSV must match that list.
CSV_FILE_FORMAT = """
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('')
"""


TABLES = {
    "website_sessions": {
        "file": "website_sessions.csv",
        "staging_table": "WEBSITE_SESSIONS_STG",
        "target_table": "WEBSITE_SESSIONS",
        "columns": [
            "WEBSITE_SESSION_ID",
            "CREATED_AT",
            "USER_ID",
            "IS_REPEAT_SESSION",
            "UTM_SOURCE",
            "UTM_CAMPAIGN",
            "UTM_CONTENT",
            "DEVICE_TYPE",
            "HTTP_REFERER",
        ],
        "keys": ["WEBSITE_SESSION_ID"],
    },
    "website_pageviews": {
        "file": "website_pageviews.csv",
        "staging_table": "WEBSITE_PAGEVIEWS_STG",
        "target_table": "WEBSITE_PAGEVIEWS",
        "columns": [
            "WEBSITE_PAGEVIEW_ID",
            "CREATED_AT",
            "WEBSITE_SESSION_ID",
            "PAGEVIEW_URL",
        ],
        "keys": ["WEBSITE_PAGEVIEW_ID"],
    },
    "orders": {
        "file": "orders.csv",
        "staging_table": "ORDERS_STG",
        "target_table": "ORDERS",
        "columns": [
            "ORDER_ID",
            "CREATED_AT",
            "WEBSITE_SESSION_ID",
            "USER_ID",
            "PRIMARY_PRODUCT_ID",
            "ITEMS_PURCHASED",
            "PRICE_USD",
            "COGS_USD",
        ],
        "keys": ["ORDER_ID"],
    },
    "order_items": {
        "file": "order_items.csv",
        "staging_table": "ORDER_ITEMS_STG",
        "target_table": "ORDER_ITEMS",
        "columns": [
            "ORDER_ITEM_ID",
            "CREATED_AT",
            "ORDER_ID",
            "PRODUCT_ID",
            "IS_PRIMARY_ITEM",
            "PRICE_USD",
            "COGS_USD",
        ],
        "keys": ["ORDER_ITEM_ID"],
    },
    "order_item_refunds": {
        "file": "order_item_refunds.csv",
        "staging_table": "ORDER_ITEM_REFUNDS_STG",
        "target_table": "ORDER_ITEM_REFUNDS",
        "columns": [
            "ORDER_ITEM_REFUND_ID",
            "CREATED_AT",
            "ORDER_ITEM_ID",
            "ORDER_ID",
            "REFUND_AMOUNT_USD",
        ],
        "keys": ["ORDER_ITEM_REFUND_ID"],
    },
    "products": {
        "file": "products.csv",
        "staging_table": "PRODUCTS_STG",
        "target_table": "PRODUCTS",
        "columns": [
            "PRODUCT_ID",
            "CREATED_AT",
            "PRODUCT_NAME",
        ],
        "keys": ["PRODUCT_ID"],
    },
}


@dag(
    dag_id="funnelvision_daily_pipeline",
    description=(
        "Bulk-load e-commerce CSV data into Snowflake, "
        "merge new records, run dbt transformations, and test the models."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "funnelvision",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=60),
    },
    tags=["funnelvision", "snowflake", "dbt", "etl"],
)
def funnelvision_daily_pipeline():

    # --------------------------------------------------
    # 1. Bulk-load CSV files into Snowflake staging tables
    # --------------------------------------------------

    @task
    def load_staging_tables():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE}"
                )

                for _, config in TABLES.items():
                    file_path = DATA_DIR / config["file"]
                    if not file_path.is_file():
                        raise FileNotFoundError(
                            f"CSV file not found: {file_path}"
                        )

                    staging_table = (
                        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
                        f"{config['staging_table']}"
                    )
                    columns = config["columns"]
                    staged_filename = f"{config['file']}.gz"

                    print(
                        f"Bulk-loading {file_path.name} into "
                        f"{staging_table}"
                    )

                    # Each attempt starts with an empty staging table.
                    cursor.execute(f"TRUNCATE TABLE {staging_table}")

                    # Upload the local container file to the named internal
                    # stage. AUTO_COMPRESS creates a .gz staged file.
                    put_sql = (
                        f"PUT file://{file_path.as_posix()} "
                        f"@{SNOWFLAKE_STAGE} "
                        "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                    )
                    cursor.execute(put_sql)

                    # Load only this file and explicitly map the CSV columns.
                    copy_sql = f"""
                        COPY INTO {staging_table}
                            ({", ".join(columns)})
                        FROM @{SNOWFLAKE_STAGE}
                        FILES = ('{staged_filename}')
                        FILE_FORMAT = ({CSV_FILE_FORMAT})
                        ON_ERROR = 'ABORT_STATEMENT'
                    """
                    cursor.execute(copy_sql)

                    result = cursor.fetchone()
                    rows_loaded = result[3] if result and len(result) > 3 else None
                    print(
                        f"COPY INTO completed for {file_path.name}; "
                        f"Snowflake reported rows_loaded={rows_loaded}"
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            conn.close()

    # --------------------------------------------------
    # 2. Merge new records into target raw tables
    # --------------------------------------------------

    @task
    def append_new_records():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()

        try:
            cursor = conn.cursor()
            try:
                for _, config in TABLES.items():
                    staging_table = (
                        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
                        f"{config['staging_table']}"
                    )
                    target_table = (
                        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
                        f"{config['target_table']}"
                    )

                    columns = config["columns"]
                    keys = config["keys"]

                    join_condition = " AND ".join(
                        f"t.{key} = s.{key}" for key in keys
                    )
                    partition_columns = ", ".join(keys)

                    merge_sql = f"""
                        MERGE INTO {target_table} AS t
                        USING (
                            SELECT *
                            FROM {staging_table}
                            QUALIFY ROW_NUMBER() OVER (
                                PARTITION BY {partition_columns}
                                ORDER BY {partition_columns}
                            ) = 1
                        ) AS s
                        ON {join_condition}
                        WHEN NOT MATCHED THEN
                            INSERT ({", ".join(columns)})
                            VALUES ({", ".join(
                                f"s.{column}" for column in columns
                            )})
                    """

                    cursor.execute(merge_sql)
                    print(
                        f"Merged new records from {staging_table} "
                        f"into {target_table}"
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        finally:
            conn.close()

    # --------------------------------------------------
    # 3. Run dbt transformations
    # --------------------------------------------------

    run_dbt = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # --------------------------------------------------
    # 4. Test dbt models
    # --------------------------------------------------

    test_dbt = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    staging = load_staging_tables()
    merge = append_new_records()

    staging >> merge >> run_dbt >> test_dbt


funnelvision_daily_pipeline()

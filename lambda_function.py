# lambda_function.py

import os
import json
import logging
import boto3
from boto3.dynamodb.types import TypeDeserializer
import snowflake.connector

from utils.flatten import flatten_json

# ─── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Environment ───────────────────────────────────────────────────────────────
DDB_TABLE      = os.getenv("DYNAMODB_TABLE")
SNOW_SECRET    = os.getenv("SNOW_SECRET_NAME")
SNOW_ACCOUNT   = os.getenv("SNOW_ACCOUNT")
SNOW_ROLE      = os.getenv("SNOW_ROLE")
SNOW_WAREHOUSE = os.getenv("SNOW_WAREHOUSE")
SNOW_DATABASE  = os.getenv("SNOW_DATABASE")
SNOW_SCHEMA    = os.getenv("SNOW_SCHEMA")
SNOW_TABLE     = os.getenv("SNOW_TABLE")
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "500"))

# ─── DynamoDB scan helper ──────────────────────────────────────────────────────
deserializer = TypeDeserializer()


def scan_dynamodb():
    """Generator that yields every item from DynamoDB as native Python."""
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    for page in paginator.paginate(TableName=DDB_TABLE):
        for item in page.get("Items", []):
            yield {k: deserializer.deserialize(v) for k, v in item.items()}


# ─── Snowflake connection & table setup ────────────────────────────────────────
def get_snowflake_secret() -> dict:
    import urllib3
    headers = {"X-Aws-Parameters-Secrets-Token": os.environ["AWS_SESSION_TOKEN"]}
    url = f"http://localhost:2773/secretsmanager/get?secretId={SNOW_SECRET}"
    resp = urllib3.PoolManager().request("GET", url, headers=headers)
    return json.loads(json.loads(resp.data)["SecretString"])


def get_snowflake_connection():
    creds = get_snowflake_secret()
    return snowflake.connector.connect(
        account   = SNOW_ACCOUNT,
        user      = creds["snow_user"],
        password  = creds["snow_pass"],
        role      = SNOW_ROLE,
        warehouse = SNOW_WAREHOUSE,
        database  = SNOW_DATABASE
    )


CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {SNOW_DATABASE}.{SNOW_SCHEMA}.{SNOW_TABLE} (
  -- we'll infer columns dynamically on first run
);
"""


def ensure_table(cur):
    # Create an empty table if this is the very first run
    cur.execute(CREATE_TABLE_DDL)


# ─── Dynamic Column Inference ──────────────────────────────────────────────────
def infer_columns(sample_record: dict) -> list[str]:
    """
    Given one nested record, flatten it and return the sorted list of
    keys. That becomes our column set.
    """
    flat = flatten_json(sample_record)
    return sorted(flat.keys())


# ─── Load Functions ────────────────────────────────────────────────────────────
def load_truncate(conn):
    cur = conn.cursor()
    # 1) ensure table exists (empty)
    ensure_table(cur)

    # 2) infer columns dynamically from the first record
    first_rec = next(scan_dynamodb(), None)
    if first_rec is None:
        logger.warning("No items in DynamoDB table.")
        return

    columns = infer_columns(first_rec)
    placeholders = ", ".join(f"%({col})s" for col in columns)
    insert_sql = (
        f"INSERT INTO {SNOW_DATABASE}.{SNOW_SCHEMA}.{SNOW_TABLE} "
        f"({', '.join(columns)}) VALUES ({placeholders})"
    )

    # 3) batch load
    batch = []
    for rec in [first_rec] + list(scan_dynamodb()):
        flat = flatten_json(rec)
        # ensure all columns present
        row = {col: flat.get(col) for col in columns}
        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            cur.executemany(insert_sql, batch)
            conn.commit()
            batch.clear()

    if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()

    cur.close()


def load_upsert(conn):
    cur = conn.cursor()
    ensure_table(cur)

    # infer columns as above
    first_rec = next(scan_dynamodb(), None)
    if first_rec is None:
        logger.warning("No items in DynamoDB table.")
        return

    columns = infer_columns(first_rec)
    sel = ", ".join(f"%({col})s AS {col}" for col in columns)
    update = ", ".join(f"T.{col}=S.{col}" for col in columns if col != "uuid")
    merge_sql = (
        f"MERGE INTO {SNOW_DATABASE}.{SNOW_SCHEMA}.{SNOW_TABLE} T "
        f"USING (SELECT {sel}) S ON T.uuid = S.uuid "
        f"WHEN MATCHED THEN UPDATE SET {update} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
        f"VALUES ({', '.join('S.'+c for c in columns)})"
    )

    for rec in scan_dynamodb():
        flat = flatten_json(rec)
        # build row dict
        row = {col: flat.get(col) for col in columns}
        cur.execute(merge_sql, row)

    conn.commit()
    cur.close()


# ─── Lambda Handlers ───────────────────────────────────────────────────────────
def truncate_load_handler(event, context):
    conn = get_snowflake_connection()
    try:
        load_truncate(conn)
        return {"statusCode": 200, "body": "Full load succeeded"}
    finally:
        conn.close()


def incremental_upsert_handler(event, context):
    conn = get_snowflake_connection()
    try:
        load_upsert(conn)
        return {"statusCode": 200, "body": "Upsert succeeded"}
    finally:
        conn.close()

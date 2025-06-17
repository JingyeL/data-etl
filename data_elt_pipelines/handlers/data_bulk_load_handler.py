import boto3
import json
import os
import logging
import psycopg2
from psycopg2 import sql

from shared.cdm_company import CdmCompany, OrderedEnum
from shared.jurisdiction import JurisdictionModel
from shared.utils import get_secret
from shared.secret import DBSecret
from shared.constants import DEFAULT_REGION


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run_query(query: str, db_secret: DBSecret) -> None:
    conn = psycopg2.connect(
        dbname=db_secret.dbname,
        user=db_secret.username,
        password=db_secret.password,
        host=db_secret.host,
        port=db_secret.port,
    )
    cur = conn.cursor()
    try:
        cur.execute(sql.SQL(query))
        conn.commit()

    except Exception as e:
        raise
    finally:
        cur.close()
        conn.close()


def get_bulk_copy_query(
    target_table_name: str,
    bucket_name: str,
    object_key: str,
    cdm_company: CdmCompany | OrderedEnum = CdmCompany,
    region: str = DEFAULT_REGION,
):
    """
    Generate the query to bulk copy the data from the S3 bucket to the staging table
    """

    target_table = f"source.{target_table_name}"
    columns = [field.value for field in cdm_company]
    query = f"""SELECT aws_s3.table_import_from_s3(
   '{target_table}',
   '{','.join(columns)}', 
   '(format csv, header true)', 
   '{bucket_name}', 
    '{object_key}', 
    '{region}');"""
    return query


def lambda_handler(event, context):
    """
    expected event payload as:
    {
        bucket: 'bucket-name',
        key: 'object-key'
    }
    """
    logger.debug(f"Received event: {json.dumps(event)}")
    aws_region = os.getenv("AWS_REGION",os.getenv("region", event.get("region", DEFAULT_REGION)))
    secret_name = os.getenv("DB_CONNECTION")
    if not secret_name:
        raise ValueError("DB_CONNECTION environment variable is required")
    secret_dict = get_secret(secret_name, aws_region=aws_region)
    logging.info("Secret retrieved successfully")
    secret = DBSecret(**secret_dict)

    logger.debug(f"Received event: {event}")
    bucket_name = event.get("bucket")
    object_key = event.get("key")
    

    s3_client = boto3.client("s3", region_name=aws_region)
    metadata = s3_client.head_object(Bucket=bucket_name, Key=object_key)["Metadata"]

    content_encoding = metadata.get("content-encoding")
    if content_encoding:
        message = f"content-encoding: {content_encoding} is not supported"
        logger.error(message)
        raise ValueError
    logger.info(f"read metadata: {metadata} from {bucket_name}/{object_key}")
    
    table_name = metadata.get("target")
    if not table_name:
        message = f"target table name is required in the metadata, {bucket_name}/{object_key} {metadata}"
        logger.error(message)
        raise ValueError(message)

    query = get_bulk_copy_query(
        table_name, bucket_name, object_key, CdmCompany, aws_region
    )
    run_query(query, secret)
    logger.info("Data loaded successfully")
    return {"statusCode": 201, "body": json.dumps("Data loaded successfully")}
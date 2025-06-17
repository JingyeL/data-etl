import json
import logging
import csv

import boto3
from botocore.exceptions import ClientError
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from awsglue.job import Job

from schema_transformation.cdm_mapping_rule import MappingRules, get_mapping_rules_key
from shared.metadata import CdmFileMetaData
from shared.content_type import ContentType
from shared.utils import get_timestamp_string
import schema_transformation.cdm_mapper as cdm_mapper
from shared.dynamodb import DynamoTable
from shared.utils import is_date_string_valid, encode_jsonb_fields
from schema_definition import cdm_schema
from shared.cdm_company import CdmCompany
from glue_utils import (
    get_spark_session_builder, 
    get_logger,
    add_missing_columns_to_dataframe
)

APP_NAME = "JL-GLUE-ETL"
num_partitions = 10
CATALOG_NAME = "oc_catalog"
CATALOG_DB = "jingyel_glue_catalog"
CHUNK_SIZE = 50000




logger = get_logger(APP_NAME)


def save_source_data(
    spark: SparkSession,
    job: Job,
    item: dict[str, any],
    datawarehouse_bucket: str,
    data: list[dict[str, any]],
    table_name: str,
    chunk_size: int = CHUNK_SIZE,
) -> str:
    """
    Save the source data to Iceberg table
    :param spark: Spark session
    :param job: Glue job
    :param item: item
    :param datawarehouse_bucket: Data warehouse bucket
    :param source_data: Source data
    :param table_name: Table name
    :param chunk_size: Chunk size
    :return: status
    """

    try:
        if data:
            encoded_data = []
            for record in data:
                encoded_data.append(encode_jsonb_fields(record))
            spark_df = spark.createDataFrame(encoded_data)
            spark_df = spark_df.repartition(num_partitions)
        logger.info(
            f"Writing to Iceberg table {CATALOG_DB}.{table_name}, row count: {spark_df.count()}"
        )
        try:
            target_schema = spark.read.table(
                f"{CATALOG_NAME}.{CATALOG_DB}.{table_name}"
            ).schema
            spark_df = add_missing_columns_to_dataframe(spark_df, target_schema)
        except Exception as e:
            logger.error(f"Error reading table {table_name}: {str(e)}")

        spark_df.write.format("iceberg").option(
            "path", f"s3://{datawarehouse_bucket}/{CATALOG_DB}/{table_name}"
        ).mode("append").saveAsTable(f"{CATALOG_NAME}.{CATALOG_DB}.{table_name}")
        job.commit()
        return "PROCESSED-LOAD-SOURCE"
    except Exception as e:
        logger.error(
            f"(LOAD-SOURCE) Error reading file s3://{item['object_key']['S']}: {str(e)}"
        )
        return "ERROR-LOAD-SOURCE"


def save_to_datalake(
    spark: SparkSession,
    job: Job,
    item: dict[str, any],
    datawarehouse_bucket: str,
    data: list[dict[str, any]],
    file_metadata: CdmFileMetaData,
    stg_table_name: str,
    mapping_rules: MappingRules,
    chunk_size: int = CHUNK_SIZE,
) -> str:
    """
    Perform schema transformation
    :param spark: Spark session
    :param job: Glue job
    :param item: item
    :param datawarehouse_bucket: Data warehouse bucket
    :param data: data
    :param metadata: metadata
    :param stg_table_name: Staging table name
    :return: status
    """

    try:
        for result_model, index in cdm_mapper.schema_transformation(
            data, mapping_rules, file_metadata, CdmCompany, chunk_size=chunk_size
        ):
            logger.info(f"Processing part {index}.  {len(result_model)} records")
            date_fields = cdm_mapper.get_date_fields(mapping_rules)
            valid = [
                row
                for row in result_model
                if all(
                    is_date_string_valid(row[date_field]) for date_field in date_fields
                )
            ]
            invalid = [
                row
                for row in result_model
                if not all(
                    is_date_string_valid(row[date_field]) for date_field in date_fields
                )
            ]
            logger.info(f"valid count {len(valid)}")
            logger.info(f"invalid count {len(invalid)}")

            encoded_data = []
            for record in valid:
                encoded_data.append(encode_jsonb_fields(record))
            df_valid = spark.createDataFrame(encoded_data, schema=cdm_schema)
            df_valid = df_valid.repartition(num_partitions)

            if spark.catalog.tableExists(
                f"{CATALOG_NAME}.{CATALOG_DB}.{stg_table_name}"
            ):
                # set table to accept schema evolution
                spark.sql(f"ALTER TABLE {CATALOG_NAME}.{CATALOG_DB}.{stg_table_name} \
                        SET TBLPROPERTIES ('allowSchemaEvolution' = 'true')")

            df_valid.write.format("iceberg").option(
                "path", f"s3://{datawarehouse_bucket}/{CATALOG_DB}/{stg_table_name}"
            ).option("mergeSchema", "true").mode("append").saveAsTable(
                f"{CATALOG_NAME}.{CATALOG_DB}.{stg_table_name}"
            )
            logger.info(
                f"Added to Iceberg table {CATALOG_DB}.{stg_table_name}, row count: {df_valid.count()}"
            )
            job.commit()

            stg_invallid_table_name = f"{stg_table_name}_invalid"
            encoded_data = []
            for record in invalid:
                encoded_data.append(encode_jsonb_fields(record))
            df_invalid = spark.createDataFrame(encoded_data, schema=cdm_schema)

            if spark.catalog.tableExists(
                f"{CATALOG_NAME}.{CATALOG_DB}.{stg_invallid_table_name}"
            ):
                # set table to accept schema evolution
                spark.sql(f"ALTER TABLE {CATALOG_NAME}.{CATALOG_DB}.{stg_invallid_table_name} \
                        SET TBLPROPERTIES ('allowSchemaEvolution' = 'true')")
            if df_invalid.count() > 0:
                df_invalid.write.format("iceberg").option(
                    "path",
                    f"s3://{datawarehouse_bucket}/{CATALOG_DB}/{stg_invallid_table_name}",
                ).option("mergeSchema", "true").mode("append").saveAsTable(
                    f"{CATALOG_NAME}.{CATALOG_DB}.{stg_invallid_table_name}"
                )
                logger.info(
                    f"Added to Iceberg table {CATALOG_DB}.{stg_invallid_table_name}, row count: {df_invalid.count()}"
                )
            else:
                logger.info("No invalid data")
            job.commit()
            return "PROCESSED-TRANSFORMATION"
    except Exception as e:
        logger.error(
            f"(LOAD-TRANSFORMATION) Error reading file s3://{item['object_key']['S']}: {str(e)}"
        )
        return "ERROR-TRANSFORMATION"




def process_file(
    spark: SparkSession,
    job: Job,
    item: dict[str, any],
    datawarehouse_bucket: str,
    dynamo_table: DynamoTable,
    config_bucket: str,
    chunk_size: int = CHUNK_SIZE,
) -> str:
    """
    save the data to Iceberg table
    :param spark: Spark session
    :param job: Glue job
    :param item: item
    :return: status
    """
    bucket_name, key = item["object_key"]["S"].split("/", 1)

    s3_client = boto3.client("s3")
    file_obj = None
    try:
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        logger.error(f"Error getting file {bucket_name}/{key}: {str(e)}")
        return "ERROR"
    if not file_obj:
        logger.error(f"File not found: {bucket_name}/{key}")
        return "ERROR"

    metadata = file_obj.get("Metadata")
    jurisdiction = metadata.get("jurisdiction")
    if not jurisdiction:
        raise ValueError("Jurisdiction is required")
    target_src_table = f"src_{jurisdiction}"
    source_content_type = file_obj.get("ContentType")
    content_type = None
    source_data = None
    if not source_content_type:
        raise ValueError("Content-Type is required")
    else:
        content_type = ContentType(source_content_type)

    if content_type == ContentType.CSV:
        file_content = file_obj["Body"].read().decode("utf-8")
        source_data = list(csv.DictReader(file_content.splitlines()))
    elif content_type == ContentType.Json_lines:
        file_content = file_obj["Body"].read().decode("utf-8")
        source_data = [json.loads(line) for line in file_content.splitlines() if line]
    else:
        raise ValueError(f"Unsupported content type: {source_content_type}")
    logger.info(
        f"Processing {jurisdiction}: s3://{item['object_key']['S']} {CATALOG_DB}.{target_src_table}"
    )

    status = save_source_data(
        spark,
        job,
        item,
        datawarehouse_bucket,
        source_data,
        target_src_table,
        chunk_size,
    )
    dynamo_table.update_item_status(item["object_key"]["S"], status)

    target_stg_table = f"stg_{jurisdiction}"
    mapping_rules_key = get_mapping_rules_key(jurisdiction, content_type)
    mapping_rules_file = s3_client.get_object(
        Bucket=config_bucket, Key=mapping_rules_key
    )
    rules_dict = json.loads(mapping_rules_file["Body"].read().decode("utf-8"))
    mapping_rules = MappingRules(**rules_dict)
    cdm_mapper_label = (
        f"{mapping_rules.meta_data.file_name}:{mapping_rules.meta_data.version}"
    )
    metadata.update(
        {
            "cdm_mapping_rules": cdm_mapper_label,
            "cdm_mapped_at": get_timestamp_string(),
        }
    )
    metadata_model = CdmFileMetaData(**metadata)
    status = save_to_datalake(
        spark,
        job,
        item,
        datawarehouse_bucket,
        source_data,
        metadata_model,
        target_stg_table,
        mapping_rules,
        chunk_size,
    )
    dynamo_table.update_item_status(item["object_key"]["S"], status)
    return status


def job_loop(
    items: list[dict[str, any]], dynamo_table: DynamoTable, args: dict[str, any]
):
    logger.info(f"Processing {[item['object_key']['S'] for item in items]} items")
    spark_session_builder = get_spark_session_builder(args.get("datawarehouse_bucket"))
    with spark_session_builder.getOrCreate() as spark:
        sc = spark.sparkContext
        glueContext = GlueContext(sc)
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        ok = False
        for item in items:
            object_key = item["object_key"]["S"]
            logger.info(f"Processing {object_key}")
            # lock the item from other workers
            try:
                ok = dynamo_table.update_item_status(
                    object_key, status="PROCESSING", pre_status="NEW"
                )
            except Exception as e:
                if "ConditionalCheckFailedException" in str(e):
                    # the record is locked by another worker
                    continue
                else:
                    logger.error(f"Failed to update item {object_key}: {str(e)}")
                    ok = False
                continue
            if ok:
                try:
                    status = process_file(
                        spark,
                        job,
                        item,
                        args.get("datawarehouse_bucket"),
                        dynamo_table,
                        args.get("config_bucket"),
                        args.get("chunk_size"),
                    )
                    dynamo_table.update_item_status(object_key, status=status)
                except Exception as e:
                    logger.error(f"Error processing file {object_key}: {str(e)}")
                    status = "ERROR"
                    dynamo_table.update_item_status(object_key, status=status)
                    raise
            items = dynamo_table.get_new_items(limit=1)
            if not items:
                logger.info("No new items to process")
                return
            else:
                return job_loop(items, dynamo_table, args)


def main():
    dynamodb_client = boto3.client("dynamodb")

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "register_table",
            "datawarehouse_bucket",
            "config_bucket",
            "chunk_size",
        ],
    )
    register_table = args.get("register_table")
    dynamo_table = DynamoTable(register_table, dynamodb_client)
    items = dynamo_table.get_new_items(limit=1)
    if not items:
        logger.info("No new items to process")
        return
    else:
        return job_loop(items, dynamo_table, args)


if __name__ == "__main__":
    main()

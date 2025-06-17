import os
import logging
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

from shared.utils import list_s3_objects


num_partitions = 10
CATALOG_NAME = "oc_catalog"
CHUNK_SIZE = 50000


def get_logger(name):
    MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def get_spark_session_builder(data_bucket: str, app_name: str) -> SparkSession:
    """
    Get a Spark session builder
    :param data_bucket: Data lake bucket
    :param app_name: Application name
    :return: Spark session
    """
    # When using Apache Iceberg with AWS Glue as the catalog (org.apache.iceberg.aws.glue.GlueCatalog),
    # Spark can directly interact with the Glue Data Catalog to manage Iceberg tables.
    # This means that Spark can create, update, and delete tables in the Glue Data Catalog without the
    # need for a separate Glue Crawler.

    configs = [
        {
            "key": "spark.sql.extensions",
            "value": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
        {
            "key": f"spark.sql.catalog.{CATALOG_NAME}",
            "value": "org.apache.iceberg.spark.SparkCatalog",
        },
        {
            "key": f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            "value": f"s3://{data_bucket}/",
        },
        {
            "key": f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl",
            "value": "org.apache.iceberg.aws.glue.GlueCatalog",
        },
        {
            "key": f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
            "value": "org.apache.iceberg.aws.s3.S3FileIO",
        },
    ]
    spark_session_builder = SparkSession.builder.appName(app_name)
    for config in configs:
        spark_session_builder = spark_session_builder.config(
            config["key"], config["value"]
        )
    return spark_session_builder


def add_missing_columns_to_dataframe(
    df: DataFrame, target_schema: StructType
) -> DataFrame:
    """
    Add missing columns to the DataFrame with null values based on the target schema.
    schema discrepancies is likely to happen when the source data is from web scrapper

    :param df: Spark DataFrame
    :param target_schema: Schema of the target table
    :return: DataFrame with missing columns added
    """
    for field in target_schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, lit(None).cast(field.dataType))
    return df


def get_source_paths(
    s3_client: boto3.client,
    object_path: str,
    suffix: list[str],
    suffix_invalid: list[str],
) -> tuple[list[str], list[str]]:
    """
    Get valid and invalid data file paths from S3.
    :param s3_client: S3 client
    :param object_path: S3 object path
    :param suffix: Suffix to filter valid objects
    :param suffix_invalid: Suffix to filter invalid objects
    :return: Tuple of valid and invalid data paths
    """
    input_s3_path = object_path
    bucket = os.path.dirname(input_s3_path).split("/")[0]
    prefix = "/".join(os.path.dirname(input_s3_path).split("/")[1:])

    valid_data_paths = list_s3_objects(s3_client, bucket, prefix, suffix=suffix)
    invalid_data_paths = list_s3_objects(
        s3_client, bucket, prefix, suffix=suffix_invalid
    )

    valid_data_files = [
        f"s3://{os.path.join(bucket, obj.path, obj.name)}" for obj in valid_data_paths
    ]
    invalid_data_files = [
        f"s3://{os.path.join(bucket, obj.path, obj.name)}" for obj in invalid_data_paths
    ]
    return valid_data_files, invalid_data_files

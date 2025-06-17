import boto3
from botocore.exceptions import ClientError
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession


from awsglue.job import Job
from shared.dynamodb import DynamoTable
from shared.param_models import JobStatus
from glue_utils import (
    get_spark_session_builder,
    get_logger,
    add_missing_columns_to_dataframe,
    get_source_paths,
    CATALOG_NAME,
)

APP_NAME = "JL-GLUE-ETL"
CATALOG_DB = "jingyel_glue_catalog"
logger = get_logger(APP_NAME)
SUFFIX = ".csv.bz2"
SUFFIX_INVALID = "invalid"


class GlueJob:
    def __init__(self, s3_data_bucket: str, job_queue_table: str) -> None:
        self.catalog = CATALOG_DB
        self.data_bucket = s3_data_bucket

        dynamodb_client = boto3.client("dynamodb")
        self.job_queue_table = DynamoTable(job_queue_table, dynamodb_client)

    def __del__(self):
        try:
            if hasattr(self, "job"):
                self.job.commit()
            if hasattr(self, "spark"):
                self.spark.stop()
        except Exception:
            pass

    def open_spark_session(self):
        """
        Open a Spark session
        :return: Spark session
        """
        self.spark = get_spark_session_builder(
            data_bucket=self.data_bucket, app_name=APP_NAME
        ).getOrCreate()
        self.glueContext = GlueContext(self.spark)
        self.job = Job(self.glueContext)
        return self.spark

    def close_spark_session(self):
        """
        Close the Spark session
        """
        try:
            if hasattr(self, "job"):
                self.job.commit()
            if hasattr(self, "spark"):
                self.spark.stop()
        except Exception:
            pass

    def table_exits(self, table_name: str) -> bool:
        """
        Check if a table exists in the Glue catalog
        :param spark: Spark session
        :param table_name: Table name
        :return: True if the table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE {self.catalog}.{table_name}")
            return True
        except Exception as e:
            return False

    def data_load(self, tasks: dict[str, any]) -> None:
        """
        data loading
        :param tasks: tasks
        :return: Job status (True if successful, False otherwise)
        tasks = {
            "prefix": {
                "stg_us_fl": ["s3://bucket/path1", "s3://bucket/path2"],
                "stg_us_fl_invalid": ["s3://bucket/path1", "s3://bucket/path2"],
            }
        }
        tasks = {
            "object_key": {
                "src_us_fl": ["object_key"]
            }
        }
        """

        for object_key, target_table_dict in tasks.items():
            for target_table, paths in target_table_dict.items():
                dynamic_frame = self.glueContext.create_dynamic_frame_from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": paths,
                        "recurse": True,
                        "groupFiles": "inPartition",
                    },
                    format="csv",
                    format_options={"withHeader": True},
                )
                df = dynamic_frame.toDF()
                logger.info(
                    f"Writing to Iceberg table {CATALOG_DB}.{target_table}, row count: {df.count()}"
                )

                try:
                    if self.spark.catalog.tableExists(
                        f"{CATALOG_NAME}.{CATALOG_DB}.{target_table}"
                    ):
                        self.spark.sql(f"ALTER TABLE {CATALOG_NAME}.{CATALOG_DB}.{target_table} \
                                SET TBLPROPERTIES ('allowSchemaEvolution' = 'true'); commit()")
                        target_schema = self.spark.table(
                            f"{CATALOG_NAME}.{self.catalog}.{target_table}"
                        ).schema
                        df = add_missing_columns_to_dataframe(df, target_schema)

                    df.write.format("iceberg").option(
                        "path", f"s3://{self.data_bucket}/{CATALOG_DB}/{target_table}"
                    ).option("mergeSchema", "true").mode("append").saveAsTable(
                        f"{CATALOG_NAME}.{CATALOG_DB}.{target_table}"
                    )
                    self.job_queue_table.update_glue_job_item_status(
                        object_key, status=JobStatus.PROCESSED.name
                    )
                    logger.info("Data loaded successfully")
                    continue
                except Exception as e:
                    logger.error(
                        f"Error in writing dataframe to {target_table} {str(e)}"
                    )
                    self.job_queue_table.update_glue_job_item_status(
                        object_key, status=JobStatus.PROCESSED.name
                    )
                    continue


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "register_table",
            "datawarehouse_bucket",
        ],
    )
    glue_job = GlueJob(args.get("datawarehouse_bucket"), args.get("register_table"))
    tasks = glue_job.job_queue_table.get_items(status=JobStatus.PREPARING.name)

    if not tasks:
        logger.info("No new task to process")
        return
    else:
        all_paths = {}
        s3_client = boto3.client("s3")

        for task in tasks:
            object_key = task["object_key"]["S"]
            target_table = task["target"]["S"]
            glue_job.job_queue_table.update_glue_job_item_status(
                object_key,
                status=JobStatus.PROCESSING.name,
                pre_status=JobStatus.PREPARING.name,
            )

            valid_data_paths, invalid_data_paths = get_source_paths(
                s3_client,
                task["object_key"]["S"],
                suffix=[SUFFIX],
                suffix_invalid=[f"{SUFFIX_INVALID}{SUFFIX}"],
            )

            target_invalid_table = f"{target_table}_{SUFFIX_INVALID}"
            if valid_data_paths:
                if object_key in all_paths.keys():
                    if target_table in all_paths[object_key].keys():
                        all_paths[object_key][target_table].extend(valid_data_paths)
                    else:
                        all_paths[object_key][target_table] = valid_data_paths
                else:
                    all_paths[object_key] = {target_table: valid_data_paths}

            if invalid_data_paths:
                if object_key in all_paths.keys():
                    if target_invalid_table in all_paths[object_key].keys():
                        all_paths[object_key][target_invalid_table].extend(
                            invalid_data_paths
                        )
                    else:
                        all_paths[object_key][target_invalid_table] = invalid_data_paths
                else:
                    all_paths[object_key] = {target_invalid_table: invalid_data_paths}

        try:
            glue_job.open_spark_session()
            glue_job.data_load(all_paths)
        except Exception as e:
            logger.error(f"Error loading {object_key} data: {str(e)}")
            raise
        finally:
            glue_job.close_spark_session()


if __name__ == "__main__":
    main()

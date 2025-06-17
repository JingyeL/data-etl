import boto3
from botocore.exceptions import ClientError
import json
from datetime import datetime
import os
import logging
import asyncio
from shared.tags import default_tags
from glue_services.glue import GlueJob
import glue_services.params as glue_params
import shared.param_models as param_models
from shared.dynamodb import DynamoTable
from shared.constants import (
    MAXIMUM_FILE_SIZE_COMPRESSED,
    DEFAULT_GLUE_CAPACITY_DPU,
    ISO8901_FORMAT,
    GLUE_JOB_MAX_RETRIES,
    GLUE_JOB_TIMEOUT_MINUTES,
)

logger = logging.getLogger()
if not logger.name:
    logger.name = __file__.split(".")[0]
logger.setLevel(logging.INFO)
GLUE_JOB_NAME = "iceberg_data_load_job"


def get_glue_job_params(
    job_name: str,
    role_arn: str,
    script_location: str,
    max_concurrent_runs: int,
    max_workers: int,
    **job_args,
) -> dict:
    description = "Glue job for loading data into the iceburg data lake"
    default_job_args = {}
    for key, value in job_args.items():
        if key == "extra_py_files":
            default_job_args["--extra-py-files"] = value
        elif key == "additional_python_modules":
            default_job_args["--additional-python-modules"] = value
        elif key == "spark_event_logs_path":
            default_job_args["--spark-event-logs-path"] = value
        else:
            default_job_args[f"--{key}"] = value

    try:
        job_command = glue_params.JobCommand(
            Name="glueetl", ScriptLocation=script_location, PythonVersion="3"
        )
        job_params = glue_params.JobParams(
            Name=job_name,
            Description=description,
            Role=role_arn,
            Command=job_command,
            NonOverridableArguments=GlueJob.glue_job_sys_args,
            DefaultArguments=default_job_args,
            WorkerType="G.1X",
            NumberOfWorkers=max_workers,
            MaxRetries=GLUE_JOB_MAX_RETRIES,
            ExecutionProperty={"MaxConcurrentRuns": max_concurrent_runs},
            Timeout=GLUE_JOB_TIMEOUT_MINUTES,
            Tags=default_tags,
        )
        return job_params
    except Exception as e:
        logger.error(f"Error creating job params: {e}")
        raise


def lambda_handler(event, context):
    """
    invoked by event notification when a new file is created in the s3 bucket or
    by schedule event
    :param event: event payload
    :param context: context
    :return: response
    """
    action = event.get("action")
    if not action:
        logger.error("Action is required")
        raise ValueError("Action is required")
    job_action = param_models.JobAction(action)
    job_queue_table = os.getenv("dynamo_table", os.getenv("dynamo_table"))
    if not job_queue_table:
        logger.error("dynamo_table environment variable is required")
        raise ValueError("dynamo_table environment variable is required")
    aws_region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))
    if not aws_region:
        logger.error("AWS region is required")
        raise ValueError("AWS region is required")

    max_dpu = int(os.getenv("capacity_dpu", DEFAULT_GLUE_CAPACITY_DPU))

    dynamodb = boto3.client("dynamodb", region_name=aws_region)
    dynamodb_client = DynamoTable(job_queue_table, dynamodb)
    if job_action == param_models.JobAction.RESET:
        # event:
        # {
        #     "action": "RESET"
        # }
        try:
            items = dynamodb_client.get_items()
            for item in items:
                dynamodb_client.update_glue_job_item_status(
                    item["object_key"]["S"],
                    "NEW",
                    timestamp=datetime.now().strftime(ISO8901_FORMAT),
                )
            return {
                "statusCode": 200,
                "body": json.dumps(f"Update all items from {job_queue_table}"),
            }
        except Exception as e:
            logger.error(
                f"Failed to delete all items from DynamoDB table {job_queue_table}. {str(e)}"
            )
            return {"statusCode": 500, "body": json.dumps(str(e))}

    elif job_action == param_models.JobAction.START:
        try:
            data_etl_msg = ""
            items = dynamodb_client.get_new_items()
            if not items:
                logger.info("No new items in job queue to process")
                return {
                    "statusCode": 200,
                    "body": json.dumps("No new items in job queue to process"),
                }
            else:
                for item in items:
                     dynamodb_client.update_glue_job_item_status(
                        key=item["object_key"]["S"],
                        status="PREPARING",
                        pre_status="NEW"
                    )
                # find the smallest file size in items
                min_file_size = min(
                    int(item["size"]["S"]) for item in items if int(item["size"]["S"])
                )
                # find the largest file size in items
                max_file_size = max(
                    int(item["size"]["S"]) for item in items if int(item["size"]["S"])
                )
                
                if (
                    max_file_size > MAXIMUM_FILE_SIZE_COMPRESSED
                    or min_file_size < 0
                ):
                    max_workers = max_dpu
                else:
                    max_workers = DEFAULT_GLUE_CAPACITY_DPU
                
                data_etl_msg += (
                    run_glue_job(job_queue_table, aws_region, max_workers, max_concurrent_runs=2)
                    + "\n"
                )
            return {"statusCode": 200, "body": json.dumps(data_etl_msg)}
        except Exception as e:
            logger.error(f"Failed to start Glue job. {str(e)}")
            return {"statusCode": 500, "body": json.dumps(str(e))}


def run_glue_job(
    job_queue_table: str, aws_region: str, workers: int, max_concurrent_runs: int
) -> str:
    load_script_location = os.getenv("load_script_location")
    if not load_script_location:
        logger.error("load_script_location is required")
        raise ValueError("load_script_location is required")

    glue_service_role_arn = os.getenv("glue_service_role_arn")
    if not glue_service_role_arn:
        logger.error("glue_service_role_arn is required")
        raise ValueError("glue_service_role_arn is required")

    s3_data_warehouse_bucket = os.getenv("s3_data_warehouse_bucket")
    if not s3_data_warehouse_bucket:
        logger.error("s3_data_warehouse_bucket is required")
        raise ValueError("s3_data_warehouse_bucket is required")

    glue_job_name = None
    try:
        job_name = f"{GLUE_JOB_NAME}_{workers}"
        glue_client = boto3.client("glue", region_name=aws_region)
        glue_job_name = GlueJob.get_job(glue_client, job_name)

    except ClientError as e:
        if "EntityNotFoundException" in str(e):
            logger.info(f"Glue job {job_name} not found. Creating.")

            job_params = get_glue_job_params(
                job_name,
                glue_service_role_arn,
                script_location=load_script_location,
                max_concurrent_runs=max_concurrent_runs,
                max_workers=workers,
                register_table=job_queue_table,
                datawarehouse_bucket=s3_data_warehouse_bucket,
                extra_py_files=os.getenv("extra_py_files"),
                additional_python_modules=os.getenv("additional_python_modules"),
                spark_event_logs_path=os.getenv("spark_event_logs_path"),
            )
            job = GlueJob(glue_client)
            try:
                glue_job_name = asyncio.run(job.create_job(job_params))

            except ClientError as e:
                logger.error(f"Failed to create Glue job {job_name}. {str(e)}")
                raise

        else:
            logger.error(f"Failed to get Glue job {job_name}. {str(e)}")
            raise

        logger.info(f"Glue job {glue_job_name} created.")

    logger.info(f"starting glue job {glue_job_name}")

    try:
        response = glue_client.start_job_run(JobName=glue_job_name, Arguments={})
        job_run_id = response.get("JobRunId")
    except Exception as e:
        logger.error(f"Failed to start Glue job {glue_job_name}. {str(e)}")
        raise
    data_etl_msg = f"Glue job {glue_job_name} started, job run id: {job_run_id}"
    logger.info(data_etl_msg)
    return data_etl_msg

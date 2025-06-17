from datetime import datetime
import json
import os
import logging
import boto3
from botocore.exceptions import ClientError

from shared.param_models import (
    JobAction,
    JobStatus,
    IngestWorkload,
    FileParam,
    SFTPData,
    LoginSecret,
)
from source_ingestion.ftp import Ftp
from shared.dynamodb import DynamoTable
from shared.utils import get_secret, list_s3_objects, files_diff, get_sftp_meta
from shared.constants import ISO8901_FORMAT
from shared.ecs_service import run_ecs_task

logger = logging.getLogger()
if not logger.name:
    logger.name = __file__.split(".")[0]
logger.setLevel(logging.INFO)


def scan_source(
    s3_client: boto3.client,
    config_key: str,
    ftp_meta: SFTPData,
    target_bucket: str,
    aws_region: str,
) -> list[FileParam]:
    """
    Scan FTP (SFTP) source for new files
    param: s3_client: boto3.client
    param: config_key: str
    param: ftp_meta: SFTPData
    param: target_bucket: str target s3 bucket for store files
    return: list[FileParam]
    """
    try:
        secret_dict = get_secret(ftp_meta.secret_name, aws_region)
        secret = LoginSecret(**secret_dict)
        logger.info("Secret retrieved successfully.")
        raw_source_files_with_timestamp = []
        raw_files_at_s3_with_timestamp = []
        files_to_download = []
        ftp_client = Ftp(secret)
    except:
        logger.error(f"Failed to get secret for {ftp_meta.secret_name}")
        raise

    try:
        # if file_name is provided, download only that file
        if ftp_meta.file_names and ftp_meta.file_pattern:
            logger.error(
                f"Both file_name and file_pattern are provided in SFTP config ({config_key}). Provide only one."
            )
            raise ValueError(
                f"Both file_name and file_pattern are provided in SFTP config ({config_key}). Provide only one."
            )
        if not ftp_meta.file_names and not ftp_meta.file_pattern:
            logger.error("Either file_name or file_pattern is required.")
            raise ValueError("Either file_name or file_pattern is required.")

        if ftp_meta.file_names:
            file_names = ftp_meta.file_names.split(",")

            raw_files = ftp_client.list_dir(
                ftp_meta.source_path,
                file_names=file_names,
                extention=None,
                include_timestamp=ftp_meta.check_timestamp,
            )
            if not ftp_meta.check_timestamp:
                raw_source_files_with_timestamp = [
                    FileParam(name=file, path=ftp_meta.source_path)
                    for file in raw_files
                ]
            else:
                raw_source_files_with_timestamp = [
                    FileParam(
                        name=file["name"],
                        path=ftp_meta.source_path,
                        timestamp=ftp_client.get_file_timestamp(
                            os.path.join(ftp_meta.source_path, file["name"])
                        ),
                    )
                    for file in raw_files
                ]
            logger.debug(
                f"Files at origin: {[file.name for file in raw_source_files_with_timestamp]}"
            )
            raw_files_at_s3_with_timestamp = list_s3_objects(
                s3_client,
                target_bucket,
                ftp_meta.target_path,
                ftp_meta.file_names,
                include_timestamp=ftp_meta.check_timestamp,
            )
            logger.debug(
                f"Files in the S3 bucket: {[file.name for file in raw_files_at_s3_with_timestamp]}"
            )
            files_to_download = files_diff(
                raw_source_files_with_timestamp,
                raw_files_at_s3_with_timestamp,
                compare_timestamp=ftp_meta.check_timestamp,
            )

        elif ftp_meta.file_pattern:
            raw_files = ftp_client.list_dir(
                ftp_meta.source_path,
                file_names=[],
                extention=ftp_meta.file_pattern,
                include_timestamp=ftp_meta.check_timestamp,
            )
            if not ftp_meta.check_timestamp:
                raw_source_files_with_timestamp = [
                    FileParam(name=file, path=ftp_meta.source_path)
                    for file in raw_files
                ]
            else:
                raw_source_files_with_timestamp = [
                    FileParam(
                        name=file["name"],
                        path=ftp_meta.source_path,
                        timestamp=ftp_client.get_file_timestamp(
                            os.path.join(ftp_meta.source_path, file["name"])
                        ),
                    )
                    for file in raw_files
                ]
            logger.debug(
                f"Files at origin: {[file.name for file in raw_source_files_with_timestamp]}"
            )
            raw_files_at_s3_with_timestamp = list_s3_objects(
                s3_client,
                target_bucket,
                ftp_meta.target_path,
                [ftp_meta.file_pattern],
                include_timestamp=ftp_meta.check_timestamp,
            )
            files_to_download = files_diff(
                raw_source_files_with_timestamp,
                raw_files_at_s3_with_timestamp,
                compare_timestamp=ftp_meta.check_timestamp,
            )
        logger.info(f"Files to download: {[file.name for file in files_to_download]}")
        return files_to_download
    except Exception as e:
        logger.error(f"Failed to get source files for download, error: {str(e)}")
        raise


def ingest_by_lambda_fuction(
    workload: IngestWorkload,
    lambda_func: str,
    payload: dict[str, str],
    dynamodb_client: DynamoTable,
    aws_region: str,
) -> None:
    """
    Start the ingest job by invoking the lambda function
    param: workload: IngestWorkload
    param: lambda_func: str
    param: payload: dict
    param: dynamodb_client: DynamoTable
    """

    dynamodb_client.update_ingest_job_status(
        workload,
        from_status=JobStatus.NEW.name,
        to_status=JobStatus.PREPARING.name,
    )

    lambda_client = boto3.client("lambda", region_name=aws_region)
    lambda_client.invoke(
        FunctionName=lambda_func,
        InvocationType="Event",
        Payload=json.dumps(payload),
    )
    return


def ingest_by_ecs(
    workload: IngestWorkload,
    instance_type: str,
    payload: dict[str, str],
    dynamodb_client: DynamoTable,
) -> None:
    """
    Start the ingest job by invoking the ECS task
    param: workload: IngestWorkload
    param: instance_type: str
    param: payload: dict
    param: dynamodb_client: DynamoTable
    """
    dynamodb_client.update_ingest_job_status(
        workload,
        from_status=JobStatus.NEW.name,
        to_status=JobStatus.PREPARING.name,
    )

    subnet_ids_string = os.getenv("PUBLIC_SUBNET_IDS")
    if not subnet_ids_string:
        logger.error("PUBLIC_SUBNET_IDS is required")
        raise ValueError("PUBLIC_SUBNET_IDS is required")
    subnet_ids = subnet_ids_string.split(",")

    sg_groups_string = os.getenv("ECS_SECURITY_GROUP")
    if not sg_groups_string:
        logger.error("ECS_SECURITY_GROUP is required")
        raise ValueError("ECS_SECURITY_GROUP is required")
    sg_groups = sg_groups_string.split(",")
    try:
        response = run_ecs_task(
            ecs_cluster=os.getenv("ECS_CLUSTER"),
            ecs_task_definition_name=os.getenv("ECS_TASK_DEFINITION"),
            ecs_container=os.getenv("ECS_CONTAINER"),
            instance_type=instance_type,
            subnet_ids=subnet_ids,
            sg_groups=sg_groups,
            payload=payload,
            module_name="fetcher_ftp_download_handler",
            entryscript="ecs_wrapper.py",
            function_name="lambda_handler",
        )
    except Exception as e:
        logger.error(f"Failed to start ECS task for {workload.object_key}")
        dynamodb_client.update_ingest_job_status(
            workload,
            from_status=JobStatus.PREPARING.name,
            to_status=JobStatus.ERROR.name,
        )
        logger.error(f"ECS task response: {str(e)}")
    if response.get("failures"):
        logger.error(f"Failed to start ECS task for {workload.object_key}")
        dynamodb_client.update_ingest_job_status(
            workload,
            from_status=JobStatus.PREPARING.name,
            to_status=JobStatus.ERROR.name,
        )
        logger.error(f"ECS task response: {response.get("failures")}")
        return


def lambda_handler(event, context):
    aws_region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))
    if not aws_region:
        logger.error("AWS region is required")
        raise ValueError("AWS region is required")

    config_bucket = os.getenv("CONFIG_BUCKET")
    if not config_bucket:
        logger.error("CONFIG_BUCKET is required")
        raise ValueError("CONFIG_BUCKET is required")
    raw_data_bucket = os.getenv("RAW_DATA_BUCKET")
    if not raw_data_bucket:
        logger.error("RAW_DATA_BUCKET is required")
        raise ValueError("RAW_DATA_BUCKET is required")
    lamdbda_func_ftp_download = os.getenv("LAMBDA_FUNC_FTP_DOWNLOAD")
    if not lamdbda_func_ftp_download:
        logger.error("LAMBDA_FUNC_FTP_DOWNLOAD is required")
        raise ValueError("LAMBDA_FUNC_FTP_DOWNLOAD is required")

    job_queue_table = os.getenv("DYNAMO_TABLE")
    if not job_queue_table:
        logger.error("DYNAMO_TABLE environment variable is required")
        raise ValueError("DYNAMO_TABLE environment variable is required")
    dynamodb = boto3.client("dynamodb", region_name=aws_region)
    dynamodb_client = DynamoTable(job_queue_table, dynamodb)

    try:
        job_action = JobAction(event.get("action"))
    except Exception:
        logger.error("Action is required")
        raise ValueError("Action is required")

    s3_client = boto3.client("s3", region_name=aws_region)
    if job_action == JobAction.ADD_WORKLOAD:
        # load all SFTP manifest data
        # scan each source and add new jobs to dynamo table
        try:
            ftp_meta_dict = get_sftp_meta(s3_client, config_bucket)
            keys = ftp_meta_dict.keys()
            for config_key, ftp_meta in ftp_meta_dict.items():
                files_to_download = scan_source(
                    s3_client, config_key, ftp_meta, raw_data_bucket, aws_region
                )

                for f in files_to_download:
                    timestamp = None
                    if isinstance(f.timestamp, int):
                        timestamp = f.timestamp
                    elif isinstance(f.timestamp, str):
                        # covert datetime string to datetime then to epoch
                        timestamp = int(
                            datetime.strptime(f.timestamp, ISO8901_FORMAT).timestamp()
                        )
                    else:
                        timestamp = 0
                    ingest_workload = IngestWorkload(
                        **{
                            "jurisdiction": ftp_meta.jurisdiction,
                            "object_key": os.path.join(f.path, f.name),
                            "periodicity": ftp_meta.periodicity,
                            "timestamp": timestamp,
                            "status": JobStatus.NEW.value,
                            "ingest_config": config_key,
                        }
                    )
                    dynamodb_client.put_item(ingest_workload.model_dump_dynamo())

        except Exception as e:
            logger.error(f"Failed to get SFTP info, error: {str(e)}")
            raise
        logger.info(
            f"Scanned following sources and added new jobs to dynamo table {keys}"
        )
        return {
            "statusCode": 200,
            "body": json.dumps("scan and add workload successfully"),
        }
    elif job_action == JobAction.START:
        # read from the dynamo table,
        # start the job depending on periodicity for the instance type
        job_items = dynamodb_client.get_new_items(limit=10)
        for job in job_items:
            ingest_workload = IngestWorkload(
                jurisdiction=job["jurisdiction"]["S"],
                object_key=job["object_key"]["S"],
                periodicity=job["periodicity"]["S"],
                timestamp=job["timestamp"]["N"] if "timestamp" in job else None,
                status=job["status"]["S"],
                ingest_config=job["ingest_config"]["S"],
            )
            logger.info(f"Starting job: {ingest_workload.object_key}")

            payload = ingest_workload.model_dump().copy()
            payload["region"] = aws_region
            payload["DYNAMO_TABLE"] = job_queue_table
            payload["RAW_DATA_BUCKET"] = raw_data_bucket
            payload["CONFIG_BUCKET"] = config_bucket
            logger.debug(f"Payload: {payload}")

            if ingest_workload.periodicity == "daily":
                # start the daily job
                try:
                    ingest_by_lambda_fuction(
                        workload=ingest_workload,
                        lambda_func=lamdbda_func_ftp_download,
                        payload=payload,
                        dynamodb_client=dynamodb_client,
                        aws_region=aws_region,
                    )
                    logger.info(
                        f"Job {ingest_workload.jurisdiction} {ingest_workload.object_key} started"
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to start daily job for {ingest_workload.object_key}, error: {str(e)}"
                    )
                    continue
                except ClientError as e:
                    logger.error(
                        f"Failed to start daily job for {ingest_workload.object_key}, error: {str(e)}"
                    )
                    continue
            else:
                try:
                    ingest_by_ecs(
                        ingest_workload,
                        instance_type="FARGATE",
                        payload=payload,
                        dynamodb_client=dynamodb_client,
                    )
                    logger.info(
                        f"Starting esc task for {ingest_workload.jurisdiction} {ingest_workload.object_key}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to start ecs task for {ingest_workload.object_key}, error: {str(e)}"
                    )
                    continue
                except ClientError as e:
                    logger.error(
                        f"Failed to start ecs task for {ingest_workload.object_key}, error: {str(e)}"
                    )
                    continue
        return {
            "statusCode": 200,
            "body": json.dumps("Job start successfully"),
        }

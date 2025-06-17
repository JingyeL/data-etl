import json
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

from source_ingestion.ftp import Ftp
from shared.utils import get_secret, get_timestamp_string, get_sftp_meta
from shared.constants import ISO8901_FORMAT, MULTI_PART_FILE_CHUNK_SIZE as CHUNK_SIZE
from shared.param_models import (
    FileParam,
    JobStatus,
    IngestWorkload,
    LoginSecret,
)
from shared.dynamodb import DynamoTable

logger = logging.getLogger()
if not logger.name:
    logger.name = __file__.split(".")[0]
logger.setLevel(logging.INFO)


def get_s3_key(target_path: str, file: FileParam) -> str:
    """
    Get target file key
    :param target_path: target path
    :param file: file name
    :param timestamp: timestamp used to create the file key
    :return: target file key
    """
    if not file.timestamp:
        timestamp = datetime.now()
    else:
        if isinstance(file.timestamp, int):
            timestamp = datetime.fromtimestamp(file.timestamp)
        elif isinstance(file.timestamp, str):
            timestamp = datetime.strptime(file.timestamp, ISO8901_FORMAT)
    formatted_date = timestamp.strftime("%Y/%m/%d")
    return f"{target_path}/{formatted_date}/{file.name}"


def sftp_to_s3_small(
    ftp_client: Ftp,
    s3_client: boto3.client,
    file_path: str,
    bucket_name: str,
    s3_key: str,
    metadata: dict[str, str],
    content_type: str,
) -> str:
    """
    download data from sftp and upload it to s3 for smaller files
    :param ftp_client: ftp client
    :param s3_client: s3 client
    :param file_path: ftp file path
    :param bucket_name: target s3 bucket name
    :param s3_key: target s3 key
    :param metadata: metadata of the file
    """

    with ftp_client.open() as sftp:
        try:
            with sftp.open(file_path, "rb") as f:
                logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
                s3_client.upload_fileobj(
                    f, bucket_name, s3_key, ExtraArgs={"Metadata": metadata}
                )
                return (
                    f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}"
                )
        except Exception as e:
            logger.error(
                f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}, error: {str(e)}"
            )
            raise e
        finally:
            try:
                f.close()
                sftp.close()
            except Exception as e:
                logger.error(
                    f"Failed to close file or sftp connection, error: {str(e)}"
                )


def multiparts_multi_threads(
    ftp_client: Ftp,
    s3_client: boto3.client,
    file_path: str,
    bucket_name: str,
    s3_key: str,
    metadata: dict[str, str],
    content_type: str,
    total_parts: int,
) -> str:
    """
    Upload large file to s3
    Note: this function causes us_fl connection to drop after 30 mins
    :param ftp_client: ftp client
    :param s3_client: s3 client
    :param file_path: ftp file path
    :param bucket_name: target s3 bucket name
    :param s3_key: target s3 key
    :param metadata: metadata of the file
    :param content_type: content type of the file
    :param total_parts: total parts of the file

    """
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name, Key=s3_key, ContentType=content_type, Metadata=metadata
    )
    upload_id = response["UploadId"]
    parts = []
    part_number = 0

    def upload_part(upload_id: int, part_number: int, data: bytes):
        part_response = s3_client.upload_part(
            Bucket=bucket_name,
            Key=s3_key,
            PartNumber=part_number + 1,  # Part numbers start at 1
            UploadId=upload_id,
            Body=data,
        )
        return {"PartNumber": part_number + 1, "ETag": part_response["ETag"]}

    try:
        with ThreadPoolExecutor(max_workers=os.getenv("MAX_WORKERS", 2)) as executor:
            downloads = [
                executor.submit(ftp_client.download_part, file_path, part_number)
                for part_number in range(total_parts)
            ]

            uploads = []
            for future in as_completed(downloads):
                part_number, data = future.result()
                if data:
                    uploads.append(
                        executor.submit(
                            upload_part,
                            s3_client,
                            bucket_name,
                            s3_key,
                            upload_id,
                            part_number,
                            data,
                        )
                    )

            for future in as_completed(uploads):
                parts.append(future.result())

            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            return f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}"

    except Exception as e:
        logger.error(
            f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}, error: {str(e)}"
        )
        s3_client.abort_multipart_upload(
            Bucket=bucket_name, Key=s3_key, UploadId=upload_id
        )
        raise


def multiparts_single_thread(
    ftp_client: Ftp,
    s3_client: boto3.client,
    file_path: str,
    bucket_name: str,
    s3_key: str,
    metadata: dict[str, str],
    content_type: str,
    total_parts: int,
) -> str:
    """
    download data from sftp and upload it to s3 in multiparts
    :param ftp_client: ftp client
    :param s3_client: s3 client
    :param file_path: ftp file path
    :param bucket_name: target s3 bucket name
    :param s3_key: target s3 key
    :param metadata: metadata of the file
    """
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name, Key=s3_key, ContentType=content_type, Metadata=metadata
    )
    upload_id = response["UploadId"]
    parts = []
    part_number = 1
    try:
        with ftp_client.open() as sftp:
            f = sftp.open(file_path, "rb")
            while True:
                data = f.read(CHUNK_SIZE)
                logger.info(f"Downloading {file_path} part: {part_number}")
                if not data:
                    f.close()
                    break

                logger.info(f"Uploading {file_path} part: {part_number}")
                part_response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data,
                )
                parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})
                part_number += 1

            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        return f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}"

    except Exception as e:
        logger.error(
            f"Failed to upload {file_path} to s3://{bucket_name}/{s3_key}, error: {str(e)}"
        )
        s3_client.abort_multipart_upload(
            Bucket=bucket_name, Key=s3_key, UploadId=upload_id
        )
        raise e
    finally:
        try:
            f.close()
            sftp.close()
        except Exception as e:
            logger.error(f"Failed to close file or sftp connection, error: {str(e)}")


def lambda_handler(event, context):
    """
    Accept calls from lambda function fetcher_ftp_hanlder
    """
    logger.debug(f"Event: {event}")
    raw_data_bucket = event.get("RAW_DATA_BUCKET", os.getenv("RAW_DATA_BUCKET"))
    if not raw_data_bucket:
        logger.error("RAW_DATA_BUCKET is required")
        raise ValueError("RAW_DATA_BUCKET is required")

    aws_region = event.get("region", os.getenv("region", os.getenv("AWS_REGION")))
    if not aws_region:
        logger.error("AWS region is required")
        raise ValueError("AWS region is required")

    job_queue_table = event.get("DYNAMO_TABLE", os.getenv("DYNAMO_TABLE"))
    if not job_queue_table:
        logger.error("DYNAMO_TABLE environment variable is required")
        raise ValueError("DYNAMO_TABLE environment variable is required")

    config_bucket = event.get("CONFIG_BUCKET", os.getenv("CONFIG_BUCKET"))
    logger.debug(f"CONFIG_BUCKET: {config_bucket}")
    if not config_bucket:
        logger.error("CONFIG_BUCKET is required")
        raise ValueError("CONFIG_BUCKET is required")

    task = IngestWorkload(**event)

    try:
        s3_client = boto3.client("s3", aws_region)
        ftp_meta_dict = get_sftp_meta(s3_client, config_bucket)
        ftp_meta = ftp_meta_dict.get(task.ingest_config)
        if not ftp_meta:
            logger.error(f"Failed to get ftp meta for {task.ingest_config}")
            raise ValueError(f"Failed to get ftp meta for {task.ingest_config}")

        secret_dict = get_secret(ftp_meta.secret_name, aws_region)
        ftp_conn = LoginSecret(**secret_dict)

    except Exception as e:
        logger.error(f"Failed to setting up ftp connection, error: {str(e)}")
        raise
    try:
        ftp_client = Ftp(ftp_conn)
        dynamodb = boto3.client("dynamodb", aws_region)
        dynamodb_client = DynamoTable(job_queue_table, dynamodb)

        dynamodb_client.update_ingest_job_status(
            workload=task,
            from_status=JobStatus.PREPARING.name,
            to_status=JobStatus.PROCESSING.name,
        )
    except ClientError as e:
        if "ConditionalCheckFailedException" in str(e):
            logger.info(
                f"Item {task.object_key} is not in the right state for work. Giveing up."
            )
            return {
                "statusCode": 500,
                "body": json.dumps(
                    f"Item {task.object_key} is not in the right state for work. Giveing up."
                ),
            }
        else:
            logger.error(f"Failed to update item {task.object_key} status. {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps(
                    f"Failed to update item {task.object_key} status. {str(e)}"
                ),
            }
    source_timestamp = None
    try:
        with ftp_client.open() as sftp:
            file_attributes = sftp.stat(task.object_key)
            file_size = file_attributes.st_size
            source_timestamp = datetime.fromtimestamp(
                file_attributes.st_mtime or file_attributes.st_ctime
            ).strftime(ISO8901_FORMAT)
    except Exception as e:
        logger.error(f"Failed to get file attributes, error: {str(e)}")
        raise

    metadata = {
        "source": "ftp",
        "jurisdiction": task.jurisdiction,
        "periodicity": task.periodicity,
        "fetched_by": "fetcher_ftp_download_handler",
        "fetched_at": get_timestamp_string(),
        "source_timestamp": source_timestamp,
        "source_name": os.path.basename(task.object_key),
    }

    s3_object_key = get_s3_key(
        ftp_meta.target_path,
        FileParam(
            path=os.path.dirname(task.object_key),
            name=os.path.basename(task.object_key),
            timestamp=source_timestamp,
        ),
    )
    logger.debug(f"target s3 object key: {s3_object_key}")

    if file_size < CHUNK_SIZE:
        message = sftp_to_s3_small(
            ftp_client=ftp_client,
            s3_client=s3_client,
            file_path=task.object_key,
            bucket_name=raw_data_bucket,
            s3_key=s3_object_key,
            metadata=metadata,
            content_type=ftp_meta.content_type,
        )
        logger.info(message)
    else:
        total_parts = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        logger.info(
            f"File size is {round(file_size/1024/1024/1024, 2)} gb, total parts: {total_parts}"
        )
        sftp_func = globals()[ftp_meta.download_strategy]
        message = sftp_func(
            ftp_client=ftp_client,
            s3_client=s3_client,
            file_path=task.object_key,
            bucket_name=raw_data_bucket,
            s3_key=s3_object_key,
            metadata=metadata,
            content_type=ftp_meta.content_type,
            total_parts=total_parts,
        )
        logger.info(message)

    dynamodb_client.update_ingest_job_status(
        workload=task,
        from_status=JobStatus.PROCESSING.name,
        to_status=JobStatus.PROCESSED.name,
    )
    logger.info(f"Successfully processed {task.jurisdiction} {task.object_key}")
    return {
        "statusCode": 200,
        "body": json.dumps(message),
    }

import zipfile
import os
import json
import logging

import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from shared.constants import MAXIMUM_FILE_SIZE_COMPRESSED, S3_CONNECTION_POOL_CONFIG


logger = logging.getLogger()
if not logger.name:
    logger.name = __file__.split(".")[0]

logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    expected event payload as:
    {
        "bucket": "bucket-name",
        "object_key": "s3 object key",
        "action": "zip" | "unzip"
        }
    """
    aws_region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))
    if not aws_region:
        logger.error("AWS region is required")
        raise ValueError("AWS region is required")

    is_lambda_runtime = True if context else False
    bucket = event.get("bucket")
    if not bucket:
        logger.error("bucket is required")
        raise ValueError("bucket is required")

    object_keys_str = event.get("object_keys")
    if not object_keys_str:
        logger.error("object_keys is required")
        raise ValueError("object_keys is required")
    source_object_keys = object_keys_str.split(",")

    action = event.get("action")
    if not action or action not in ["zip", "unzip"]:
        logger.error("action is required")
        raise ValueError("action is required")

    s3_client = boto3.client(
        "s3", region_name=aws_region, config=S3_CONNECTION_POOL_CONFIG
    )
    if action == "zip":
        raise NotImplementedError("zip action is not implemented")

    elif action == "unzip":
        size = 0
        all_metadata = {}
        for soure_object_key in source_object_keys:
            response = s3_client.head_object(Bucket=bucket, Key=soure_object_key)
            size += int(response["ContentLength"])
            all_metadata[soure_object_key] = response["Metadata"]
        if is_lambda_runtime and size > MAXIMUM_FILE_SIZE_COMPRESSED:
            message = (
                f"File size {size} is greater than 100MB, can not handle it via lambda"
            )
            logger.error(
                f"File size {size} is greater than 100MB, can not handle it via lambda"
            )
            return {"statusCode": 400, "body": json.dumps(message)}

        def upload_task(
            unzipped_file: str,
            object_prefix: str,
            extract_to: str,
            metadata: dict,
            source_name_meta: str | None = None,
        ):
            s3_key = os.path.join(object_prefix, unzipped_file)
            if source_name_meta:
                copy_of_metadata = metadata.copy()
                copy_of_metadata["source_name"] = source_name_meta
            else:
                copy_of_metadata = metadata

            upload_file(
                s3_client,
                bucket,
                s3_key,
                os.path.join(extract_to, unzipped_file),
                copy_of_metadata,
            )
            logger.info(f"Uploaded file {s3_key}")
            del copy_of_metadata

        extract_to = "/tmp"
        for soure_object_key in source_object_keys:
            downloaded_file_path = download_file(s3_client, bucket, soure_object_key)
            logger.info(f"Downloaded file {soure_object_key} to {downloaded_file_path}")
            unzipped_files = unzip_file(downloaded_file_path, extract_to=extract_to)
            logger.info(f"Unzipped files: {unzipped_files}")

            with ThreadPoolExecutor() as executor:
                for unzipped_file in unzipped_files:
                    executor.submit(
                        upload_task,
                        unzipped_file,
                        os.path.dirname(soure_object_key),
                        extract_to,
                        all_metadata[soure_object_key],
                        source_name_meta=os.path.basename(unzipped_file).split(".")[0]
                    )
    return {"statusCode": 200, "body": json.dumps("Files unzipped successfully")}


def download_file(s3_client, bucket, object_key):
    try:
        file_path = f"/tmp/{os.path.basename(object_key)}"
        s3_client.download_file(bucket, object_key, file_path)
        return file_path
    except ClientError as e:
        logger.error(f"Failed to download file {object_key}. {str(e)}")
        raise


def upload_file(
    s3_client: boto3.client,
    bucket: str,
    object_key: str,
    file_path: str,
    metadata: dict[str, str] | None = None,
):
    try:
        s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket,
            Key=object_key,
            ExtraArgs={"Metadata": metadata},
        )
    except ClientError as e:
        logger.error(f"Failed to upload file {object_key}. {str(e)}")
        raise


def zip_files(file_paths, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in file_paths:
            zipf.write(file_path, os.path.basename(file_path))


def unzip_file(zip_path, extract_to):
    extracted_files = []
    with zipfile.ZipFile(zip_path, "r") as zipf:
        zipf.extractall(extract_to)
        extracted_files = zipf.namelist()
    return extracted_files

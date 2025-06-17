import boto3
import json
import logging
import os
from shared.utils import list_s3_objects


logger = logging.getLogger()
logger.name = __file__.split(".")[0]

logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    """
    expected event payload as:
    {
        "bucket": "bucket-name",
        "prefix": "s3 object prefix",
        "suffix": "s3 object suffix",
        "field_name": "metadata_name",
        "fiel_value": "metadata_value",
        "dry_run": True|False,
        "overwrite": True|False
    }
    """
                             
    region = os.getenv("region")
    bucket = event.get("bucket")
    prefix = event.get("prefix")
    suffix = event.get("suffix")
    field_name = event.get("field_name")
    field_value = event.get("field_value")
    dry_run = True if event.get("dry_run") == "True" else False
    overwrite = True if event.get("overwrite") == "True" else False
    logger.info(f"Received event: {event}")

    s3_client = boto3.client("s3", region_name=region)
    file_objects = list_s3_objects(s3_client, bucket, prefix, [suffix])
    for file_param in file_objects:
        key = os.path.join(file_param.path, file_param.name)
        response = s3_client.head_object(Bucket=bucket, Key=key)
        metadata = response.get("Metadata", {})
        original_content_type = response.get("ContentType")
        if not overwrite and field_name in metadata and metadata.get(field_name):
            logger.info(f"Metadata for {key} already exists, skipping")
            continue

        if field_name=="source_name":
            field_value = file_param.name
            if dry_run:
                logger.info(
                    f"(Dry run) Metadata for {key} would be updated - '{field_name}={field_value}'"
                )
            else:
                metadata[field_name] = field_value
                s3_client.copy_object(
                    Bucket=bucket,
                    Key=key,
                    CopySource={"Bucket": bucket, "Key": key},
                    Metadata=metadata,
                    MetadataDirective="REPLACE",
                    ContentType=original_content_type
                )
                logger.info(
                    f"Updated metadata for {key} - '{field_name}={field_value}'"
                )
        elif field_value: 
            if dry_run:
                logger.info(
                    f"(Dry run) Metadata for {key} would be updated - '{field_name}={field_value}'"
                )
            else:
                metadata[field_name] = field_value
                s3_client.copy_object(
                    Bucket=bucket,
                    Key=key,
                    CopySource={"Bucket": bucket, "Key": key},
                    Metadata=metadata,
                    MetadataDirective="REPLACE",
                    ContentType=original_content_type
                )
                logger.info(
                    f"Updated metadata for {key} - '{field_name}={field_value}'"
                )
        else:
            logger.info(f"Field value is empty for {key}")
    logger.info(f"Metadata updated for {len(file_objects)} objects")
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Metadata updated successfully"}),
    }

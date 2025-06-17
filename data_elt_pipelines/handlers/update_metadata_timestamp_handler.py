import boto3
import logging
import os
from shared.utils import list_s3_objects
from datetime import datetime


logger = logging.getLogger()
if not logger.name:
    logger.name = __file__.split(".")[0]
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    expected event payload as:
    {
        "bucket": "bucket-name",
        "prefix": "s3 object prefix",
        "suffix": "s3 object suffix",
        "fields": ["fetched_at", "parsed_at", "cdm_created_at", "source_timestamp"],
        "dry_run": True|False
    }
    """
                             
    region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))
    if not region:
        logger.error("region is required")
        raise ValueError("region is required")
    
    bucket = event.get("bucket")
    prefix = event.get("prefix")
    suffix = event.get("suffix")

    object_keys = event.get("object_keys",[])
    if not isinstance(object_keys, list) or not all(isinstance(key, str) for key in object_keys):
        raise ValueError("The 'object_keys' parameter must be a list of strings)")                     

    fields = event.get("fields", [])
    if not isinstance(fields, list) or not all(isinstance(field, str) for field in fields):
        raise ValueError("The 'fields' parameter must be a list of strings.")
    
    dry_run = True if event.get("dry_run") == "True" else False
    logger.info(f"Received event: {event}")

    s3_client = boto3.client("s3", region_name=region)
    if object_keys:
        file_objects = object_keys
    else:
        file_objects = list_s3_objects(s3_client, bucket, prefix, [suffix])
    logger.info(f"Found {len(file_objects)} objects in {bucket} with prefix {prefix} and suffix {suffix}")
    for file_param in file_objects:
        key = os.path.join(file_param.path, file_param.name)
        file_obj = s3_client.get_object(Bucket=bucket, Key=key)
        original_content_type = file_obj.get("ContentType")
        metadata = file_obj.get("Metadata", {})
        for field_name in fields:
            if field_name in metadata.keys():
                try:
                    # Convert datetime string to remove millisecond and nanosecond
                    original_value = metadata[field_name]
                    try:
                        dt = datetime.strptime(original_value, "%Y-%m-%dT%H:%M:%S.%f")
                    except ValueError:
                        try:
                            dt = datetime.strptime(original_value, "%Y-%m-%dT%H:%M:%S.%f%z")
                        except ValueError:
                            try:
                                dt = datetime.strptime(original_value, "%Y-%m-%dT%H:%M:%S")
                            except ValueError:
                                try:
                                    dt = datetime.strptime(original_value, "%Y-%m-%dT%H:%M:%SZ")
                                except ValueError:
                                    dt = datetime.strptime(original_value, "%Y-%m-%d %H:%M:%S")
                                    
                            
                    metadata[field_name] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                except ValueError:
                    logger.warning(f"Field {field_name} does not match expected datetime format: {metadata[field_name]}")
                
        else:
            field_value = os.path.basename(key)
            metadata[field_name] = field_value
        if not dry_run:
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
            logger.info(
                f"(Dry run) Metadata for {key} would be updated - '{field_name}={field_value}'"
            )

import boto3
import io
import json
import logging
import os
import bz2
from botocore.exceptions import ClientError
from datetime import datetime
import raw_parsers.fixed_width_txt as parser
from shared.metadata import ConfigMetadata
from shared.utils import get_timestamp_string, get_target_s3_object_key
from shared.content_type import ContentType
from shared.param_models import JobAction
from shared.constants import (
    ISO8901_FORMAT,
    MAXIMUM_FILE_SIZE_COMPRESSED,
    DEFAULT_DATA_CHUNK_SIZE,
    MULTI_PART_FILE_CHUNK_SIZE,
)
from shared.ecs_service import run_ecs_task
from shared.dynamodb import add_glue_job_task


module_name = os.path.basename(__file__).split(".")[0]
logger = logging.getLogger()
if not logger.name:
    logger.name = module_name
logger.setLevel(logging.INFO)


def process_with_ecs(
    object_key: str,
    from_bucket: str,
    **kwargs,
) -> None:
    """
    Process file with ECS task, by default it will chunk the file into smaller size using environment variables
    :param object_key: object key
    :param from_bucket: from bucket
    :param kwargs: keyword arguments
    """
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

    ecs_cluster = os.getenv("ECS_CLUSTER")
    if not ecs_cluster:
        logger.error("ECS_CLUSTER is required")
        raise ValueError("ECS_CLUSTER is required")

    ecs_task_def = os.getenv("ECS_TASK_DEFINITION")
    if not ecs_task_def:
        logger.error("ECS_TASK_DEFINITION is required")
        raise ValueError("ECS_TASK_DEFINITION is required")

    ecs_container = os.getenv("ECS_CONTAINER")
    if not ecs_container:
        logger.error("ECS_CONTAINER is required")
        raise ValueError("ECS_CONTAINER is required")

    chunk_size = int(os.getenv("chunk_size", DEFAULT_DATA_CHUNK_SIZE))

    payload = {
        "key": object_key,
        "action": "START",
        "raw_data_bucket": from_bucket,
        "config_bucket": kwargs.get("rules_bucket"),
        "region": kwargs.get("aws_region"),
        "chunk_size": chunk_size,
    }
    if kwargs.get("source_data_bucket"):
        payload.update({"source_data_bucket": kwargs.get("source_data_bucket")})
    if kwargs.get("rules"):
        payload.update({"rules": kwargs.get("rules")})

    response = run_ecs_task(
        ecs_cluster=ecs_cluster,
        ecs_task_definition_name=ecs_task_def,
        ecs_container=ecs_container,
        instance_type="FARGATE",
        subnet_ids=subnet_ids,
        sg_groups=sg_groups,
        payload=payload,
        module_name=module_name,
        entryscript="ecs_wrapper.py",
        function_name="lambda_handler",
        memory=kwargs.get("memory"),
    )
    logger.info(f"ecs task response: {response}")


def process_file(
    object_key: str,
    raw_data_bucket: str,
    source_data_bucket: str,
    s3_client: boto3.client,
    **kwargs,
) -> str:
    """
    Process fixed width text file, by default it will not chunk the file
    :param object_key: object key
    :param raw_data_bucket: raw data bucket
    :param source_data_bucket: source data bucket
    :param s3_client: s3 client
    :param kwargs: keyword arguments
    :return: target object key or prefix
    """
    try:
        file_obj = s3_client.get_object(Bucket=raw_data_bucket, Key=object_key)
        metadata = file_obj.get("Metadata")
        if not metadata:
            raise ValueError("meta data is required")

    except Exception as e:
        logger.error(f"Failed to get file: {object_key} error: {str(e)}")
        raise
    except ClientError as e:
        logger.error(f"Failed to get file: {object_key} error: {str(e)}")
        raise

    jurisdiction = metadata.get("jurisdiction")
    if not jurisdiction:
        raise ValueError("meta data jurisdiction is required")

    if not kwargs.get("rules") and not kwargs.get("rules_bucket"):
        raise ValueError("rules or rules_bucket are required")
    rules = {}
    if not kwargs.get("rules"):
        rules = get_fixed_width_field_def(
            jurisdiction, kwargs.get("rules_bucket"), s3_client
        )
    else:
        rules = kwargs.get("rules")

    def_file_meta = ConfigMetadata(**rules.get("meta_data"))
    metadata.update(
        **{
            "parsed_by": f"{def_file_meta.file_name}_{def_file_meta.version}",
            "parsed_at": get_timestamp_string(),
            "target": f"src_{jurisdiction}",
        }
    )
    chunk_size = kwargs.get("chunk_size", 0)

    if file_obj["ContentLength"] > MULTI_PART_FILE_CHUNK_SIZE:
        logger.info(f"Chunk size {chunk_size}, will use chunking")
        return transform_to_csv_chunks(
            object_key,
            source_data_bucket,
            file_obj,
            metadata,
            rules,
            s3_client,
            chunk_size,
        )
    else:
        logger.info(f"transform_to_csv_chunks chunk size {chunk_size})")
        return transform_to_csv(
            object_key, source_data_bucket, file_obj, metadata, rules, s3_client
        )


def transform_to_csv_chunks(
    source_obj_key: str,
    target_bucket: str,
    file_obj: dict,
    metadata: dict,
    rules: dict,
    s3_client: boto3.client,
    chunk_size: int = DEFAULT_DATA_CHUNK_SIZE,
) -> tuple[str, int]:
    """
    Transform fixed width text file to a number of compressed CSV files
    :param source_obj_key: source object key
    :param target_bucket: target bucket
    :param data: data
    :param metadata: metadata
    :param rules: rules
    :param s3_client: s3 client
    :return: target objects prefix
    """
    prefix = source_obj_key.split("/")[0]
    file_content = io.StringIO(
        file_obj["Body"].read().decode("utf-8", errors="replace")
    )
    for data_string_io, index in parser.parse_chunks(
        file_content, rules, add_hash=True, chunk_size=chunk_size
    ):
        file_key = get_target_s3_object_key(
            prefix=prefix,
            timestamp=datetime.strptime(
                metadata.get("source_timestamp"), ISO8901_FORMAT
            ),
            file_key=metadata.get("source_name"),
            index=index,
            add_file_name_as_prefix=True,
        )
        target_object_key = f"{file_key}.bz2"
        data_string_io.seek(0)
        body = bz2.compress(data_string_io.getvalue().encode("utf-8"))
        try:
            s3_client.put_object(
                Bucket=target_bucket,
                Key=target_object_key,
                Body=body,
                Metadata=metadata,
                ContentType=ContentType.CSV.value,
                ContentEncoding="bzip2",
            )
            logger.info(f"File {target_object_key} created successfully.")
        except Exception as e:
            logger.error(f"Failed to put file: {target_object_key} error: {str(e)}")
            raise
        except ClientError as e:
            logger.error(f"Failed to put file: {target_object_key} error: {str(e)}")
            raise
    return os.path.dirname(target_object_key), -1


def transform_to_csv(
    source_obj_key: str,
    target_bucket: str,
    file_obj: dict,
    metadata: dict,
    rules: dict,
    s3_client: boto3.client,
) -> tuple[str, int]:
    """
    Transform fixed width text file to CSV
    :param source_obj_key: source object key
    :param target_bucket: target bucket
    :param data: data
    :param metadata: metadata
    :param rules: rules
    :param s3_client: s3 client
    :return: target object key and byte size
    """
    # a jurisdiction can have multiple prefixes, such like us_fl, us_fl_historical
    # use original file key to get the correct prefix
    prefix = source_obj_key.split("/")[0]
    file_key = get_target_s3_object_key(
        prefix=prefix,
        timestamp=datetime.strptime(metadata.get("source_timestamp"), ISO8901_FORMAT),
        file_key=metadata.get("source_name"),
        index=None,
        add_file_name_as_prefix=False,
    )

    file_content = io.StringIO(
        file_obj["Body"].read().decode("utf-8", errors="replace")
    )
    try:
        data_string_io = parser.parse(file_content, rules)
    except Exception as e:
        logger.error(f"Failed to parse file: {source_obj_key} error: {str(e)}")
        raise

    data_string_io.seek(0)
    target_object_key = f"{file_key}.bz2"
    body = bz2.compress(data_string_io.getvalue().encode("utf-8"))
    # get bytes size
    byte_size = len(body)

    try:
        s3_client.put_object(
            Bucket=target_bucket,
            Key=target_object_key,
            Body=body,
            Metadata=metadata,
            ContentType=ContentType.CSV.value,
            ContentEncoding="bzip2",
        )
        logger.info(f"File {target_object_key} created successfully.")
    except Exception as e:
        logger.error(f"Failed to put file: {target_object_key} error: {str(e)}")
        raise
    except ClientError as e:
        logger.error(f"Failed to put file: {target_object_key} error: {str(e)}")
        raise

    return target_object_key, byte_size


def get_fixed_width_field_def(
    jurisdiction: str, bucket: str, s3_client: boto3.client
) -> dict:
    """
    Get fixed width field definition
    :param jurisdiction: jurisdiction
    :return: fixed width field definition
    """
    fixed_width_field_def = f"fixed_width_field_def/{jurisdiction.lower()}/latest/{jurisdiction.lower()}.json"

    try:
        response = s3_client.get_object(Bucket=bucket, Key=fixed_width_field_def)
    except Exception as e:
        logger.error(
            f"Failed to get fixed width field definition: {fixed_width_field_def}, error: {str(e)}"
        )
        raise
    except ClientError as e:
        logger.error(
            f"Failed to get fixed width field definition: {fixed_width_field_def}, error: {str(e)}"
        )
        raise
    return json.loads(response["Body"].read().decode("utf-8", errors="replace"))


def lambda_handler(event, context):
    """
    expected event payload as: action is optional and jurisdiction is optional
    {
        "bucket": "bucket-name",
        "object": "key": "object-key",
        "action": "reload",
        "jurisdiction": "us_fl"
    }

    """
    logger.debug(f"Received event: {json.dumps(event)}")
    is_lambda_runtime = True if context else False
    raw_data_bucket = event.get("raw_data_bucket", os.getenv("RAW_DATA_BUCKET"))
    if not raw_data_bucket:
        logger.error("raw_data_bucket is required")
        raise ValueError("raw_data_bucket is required")

    try:
        job_action = JobAction(event.get("action"))
    except Exception:
        logger.error("action is required")
        raise ValueError("action is required")

    raw_file_key = event.get("key")
    if job_action == JobAction.START and not raw_file_key:
        logger.error("(object) key is required")
        raise ValueError("(object) key is required")

    config_bucket = event.get("config_bucket", os.getenv("CONFIG_BUCKET"))
    if not config_bucket:
        logger.error("config_bucket is required")
        raise ValueError("config_bucket is required")

    source_data_bucket = event.get(
        "source_data_bucket", os.getenv("SOURCE_DATA_BUCKET")
    )
    if not source_data_bucket:
        logger.error("source_data_bucket is required")
        raise ValueError("env var source_data_bucket is required")

    aws_region = event.get("region", os.getenv("region", os.getenv("AWS_REGION")))
    if not aws_region:
        logger.error("region is required")
        raise ValueError("region is required")

    s3_client = boto3.client("s3", region_name=aws_region)

    if job_action == JobAction.START:
        try:
            response = s3_client.head_object(Bucket=raw_data_bucket, Key=raw_file_key)
            size = int(response["ContentLength"])
            jurisdiction = response["Metadata"].get("jurisdiction")
        except Exception as e:
            logger.error(f"Failed to get file: {raw_file_key} error: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {"message": f"Failed to get file: {raw_file_key} error: {str(e)}"}
                ),
            }
        except ClientError as e:
            logger.error(f"Failed to get file: {raw_file_key} error: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {"message": f"Failed to get file: {raw_file_key} error: {str(e)}"}
                ),
            }

        if not jurisdiction:
            logger.error("jurisdiction is required")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "jurisdiction is required"}),
            }

        if size > MAXIMUM_FILE_SIZE_COMPRESSED and is_lambda_runtime:
            # use this lambda runtime to launch ecs task
            logger.info(
                f"File size {size} is greater than 100MB, will use ecs task instead"
            )
            try:
                process_with_ecs(
                    raw_file_key,
                    from_bucket=raw_data_bucket,
                    s3_client=s3_client,
                    source_data_bucket=source_data_bucket,
                    rules_bucket=config_bucket,
                    rules=event.get("rules"),
                    aws_region=aws_region,
                    memory=os.getenv("ecs_container_memory_size"),
                )
                logger.info(f"Starting esc task for {raw_file_key}")
                return {
                    "statusCode": 201,
                    "body": json.dumps(
                        {"message": f"Starting esc task for {raw_file_key}"}
                    ),
                }
            except Exception as e:
                logger.error(
                    f"Failed to start ecs task for {raw_file_key}, error: {str(e)}"
                )
                raise
            except ClientError as e:
                logger.error(
                    f"Failed to start ecs task for {raw_file_key}, error: {str(e)}"
                )
                raise

        # we are running in ecs task, or file size is less than 100MB
        try:
            key, size = process_file(
                raw_file_key,
                raw_data_bucket=raw_data_bucket,
                source_data_bucket=source_data_bucket,
                s3_client=s3_client,
                rules_bucket=config_bucket,
                chunk_size=int(event.get("chunk_size", os.getenv("chunk_size", 0))),
            )
        except Exception as e:
            logger.error(f"Failed to process file: {raw_file_key} error: {str(e)}")
            raise
        try:
            add_glue_job_task(source_data_bucket, key, size, f"src_{jurisdiction}")
        except ClientError as e:
            logger.error(
                f"Failed to add glue job task for {raw_file_key}, error: {str(e)}"
            )

    return {
        "statusCode": 201,
        "body": json.dumps({"message": f"File {raw_file_key} processed successfully"}),
    }

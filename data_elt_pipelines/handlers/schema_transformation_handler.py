import boto3
import asyncio
import bz2
import csv
import io
import json
import logging
import os
from shared.ecs_service import run_ecs_task
from typing import Any
from botocore.exceptions import ClientError

from datetime import datetime
from schema_transformation.cdm_mapping_rule import (
    MappingRules,
    get_mapping_rules_key,
)
from shared.metadata import CdmFileMetaData
from shared.content_type import ContentType
from shared.utils import (
    get_timestamp_string,
    get_target_s3_object_key,
    is_date_string_valid,
)
from shared.constants import (
    MAXIMUM_FILE_SIZE_COMPRESSED,
    ISO8901_FORMAT,
    S3_CONNECTION_POOL_CONFIG,
)
import schema_transformation.cdm_mapper as cdm_mapper
from shared.cdm_company import CdmCompany
from shared.dynamodb import add_glue_job_task 


module_name = os.path.basename(__file__).split(".")[0]
logger = logging.getLogger()
if not logger.name:
    logger.name = module_name
logger.setLevel(logging.INFO)


def process_with_ecs(
    object_key: str,
    from_bucket: str,
    to_bucket: str,
    config_bucket: str,
    aws_region: str,
    memory: int | None = None,
    chunk_size: int = cdm_mapper.DEFAULT_MAX_DATA_CHUNK_SIZE,
) -> None:
    """
    Process file with ECS task
    :param object_key: Source object key
    :param from_bucket: From bucket
    :param to_bucket: To bucket
    :param config_bucket: Config bucket
    :param aws_region: AWS region
    :param memory: Memory override
    :param chunk_size: Chunk size
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

    payload = {
        "key": object_key,
        "bucket": from_bucket,
        "chunk_size": chunk_size,
        "config_bucket": config_bucket,
        "cdm_data_bucket": to_bucket,
        "region": aws_region,
    }

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
        memory=memory,
    )
    logger.info(f"ecs task response: {response}")


def lambda_handler(event, context):
    """
    expected event payload as:
    {
        "bucket": "bucket-name",
        "object": {
            "key": "object-key"
        }
    """
    logger.debug(f"Received event: {json.dumps(event)}")
    is_lambda_runtime = True if context else False

    config_bucket = event.get("config_bucket", os.getenv("CONFIG_BUCKET"))
    if not config_bucket:
        raise ValueError("CONFIG_BUCKET is required")

    cdm_data_bucket = event.get("cdm_data_bucket", os.getenv("CDM_DATA_BUCKET"))
    if not cdm_data_bucket:
        raise ValueError("CDM_DATA_BUCKET is required")

    source_data_bucket = event.get("bucket")
    if not source_data_bucket:
        raise ValueError("bucket is required")

    source_object_key = event.get("key")
    if not source_object_key:
        raise ValueError("(object) key is required")

    chunk_size = event.get("chunk_size", os.getenv("chunk_size"))
    if not chunk_size:
        chunk_size = cdm_mapper.DEFAULT_MAX_DATA_CHUNK_SIZE
    else:
        chunk_size = int(chunk_size)

    logger.info(
        f"Processing file: {source_object_key}, from bucket: {source_data_bucket}, chunk size: {chunk_size}"
    )

    # for if it is running in ecs context
    aws_region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))

    if not aws_region:
        logger.error("region is required")
        raise ValueError("region is required")
    s3_client = boto3.client(
        "s3", region_name=aws_region, config=S3_CONNECTION_POOL_CONFIG
    )

    if is_lambda_runtime:
        logger.info("Running in lambda environment")

        try:
            response = s3_client.head_object(
                Bucket=source_data_bucket, Key=source_object_key
            )
            size = int(response["ContentLength"])
        except Exception as e:
            logger.error(f"Failed to get file: {source_object_key} error: {str(e)}")
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "message": f"Failed to get file: {source_object_key} error: {str(e)}"
                    }
                ),
            }
        if size > MAXIMUM_FILE_SIZE_COMPRESSED and is_lambda_runtime:
            message = (
                f"File size {size} is greater than 100MB, will use ecs task instead"
            )
            logger.info(message)
            try:
                process_with_ecs(
                    source_object_key,
                    from_bucket=source_data_bucket,
                    to_bucket=cdm_data_bucket,
                    config_bucket=config_bucket,
                    aws_region=aws_region,
                    memory=os.getenv("ecs_container_memory_size"),
                    chunk_size=chunk_size,
                )

                logger.info(f"Starting esc task for {source_object_key}")
                return {
                    "statusCode": 201,
                    "body": json.dumps(
                        {"message": f"Starting esc task for {source_object_key}"}
                    ),
                }
            except Exception as e:
                logger.error(
                    f"Failed to start ecs task for {source_object_key}, error: {str(e)}"
                )
                raise
            except ClientError as e:
                logger.error(
                    f"Failed to start ecs task for {source_object_key}, error: {str(e)}"
                )
                raise
    file_obj = s3_client.get_object(Bucket=source_data_bucket, Key=source_object_key)

    metadata = file_obj.get("Metadata", {})
    logger.debug(f"meta data: {metadata}")

    source_content_type = None
    s_content_type = file_obj.get("ContentType")
    if not s_content_type:
        raise ValueError("Content-Type is required")
    else:
        source_content_type = ContentType(s_content_type)

    jurisdiction = file_obj["Metadata"].get("jurisdiction")
    if not jurisdiction:
        raise ValueError("meta data jurisdiction is required")

    if source_content_type == ContentType.CSV:
        if "ContentEncoding" in file_obj:
            if file_obj.get("ContentEncoding") == "bzip2":
                file_content = bz2.decompress(file_obj["Body"].read()).decode("utf-8")
            elif file_obj.get("ContentEncoding") == "gzip":
                logger.error("Gzip compression is not supported")
            else:
                logger.error("The only compression supported is bzip2")
        else:
            file_content = file_obj["Body"].read().decode("utf-8")

        source_data = list(csv.DictReader(file_content.splitlines()))
    elif source_content_type == ContentType.Json_lines:
        file_content = file_obj["Body"].read().decode("utf-8")
        source_data = [json.loads(line) for line in file_content.splitlines() if line]
    else:
        raise ValueError(f"supported content type: {source_content_type}")

    key = get_mapping_rules_key(jurisdiction, source_content_type)
    mapping_rules_file = s3_client.get_object(Bucket=config_bucket, Key=key)
    rules_dict = json.loads(mapping_rules_file["Body"].read().decode("utf-8"))
    mapping_rules = MappingRules(**rules_dict)

    metadata.update(
        {
            "cdm_mapping_rules": f"{mapping_rules.meta_data.file_name}:{mapping_rules.meta_data.version}",
            "cdm_mapped_by": f"{__name__}",
            "cdm_mapped_at": get_timestamp_string(),
        }
    )
    file_metadata = CdmFileMetaData(**metadata)
    # a jurisdiction can have multiple data sources
    # get prefix from source_object_filename
    date_fields = cdm_mapper.get_date_fields(mapping_rules)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    if loop.is_running():
        asyncio.ensure_future(
            create_cdm_model(
                s3_client,
                cdm_data_bucket,
                source_object_key,
                source_content_type,
                source_data,
                jurisdiction,
                mapping_rules,
                file_metadata,
                date_fields,
                chunk_size,
            )
        )
    else:
        loop.run_until_complete(
            create_cdm_model(
                s3_client,
                cdm_data_bucket,
                source_object_key,
                source_content_type,
                source_data,
                jurisdiction,
                mapping_rules,
                file_metadata,
                date_fields,
                chunk_size,
            )
        )

    return {
        "statusCode": 200,
        "body": json.dumps("ok"),
    }


async def create_cdm_model(
    s3_client: boto3.client,
    target_bucket: str,
    source_obj_key: str,
    target_content_type: ContentType,
    data: list[dict],
    jurisdiction: str,
    mapping_rules: MappingRules,
    file_metadata: CdmFileMetaData,
    date_fields: list[str],
    chunk_size: int,
):
    """
    Create CDM model and save the result to S3.

    :param target_bucket: Target bucket
    :param source_obj_key: Source object key
    :param target_content_type: Target content type
    :param data: Data to be transformed
    :param jurisdiction: Jurisdiction
    :param mapping_rules: Mapping rules
    :param file_metadata: File metadata
    :param date_fields: Date fields
    :param chunk_size: Chunk size
    """
    prefix = source_obj_key.split("/")[0]

    tasks = []
    for result_model, index in cdm_mapper.schema_transformation(
        data, mapping_rules, file_metadata, CdmCompany, chunk_size
    ):
        logger.info(f"Processing index: {index}")
        try:
            task = asyncio.create_task(
                process_result(
                    target_bucket,
                    s3_client,
                    file_metadata,
                    target_content_type,
                    jurisdiction,
                    prefix,
                    date_fields,
                    result_model,
                    index=index,
                    filename=os.path.basename(source_obj_key)
                )
            )
            tasks.append(task)
        except Exception as e:
            logger.error(f"Error processing {source_obj_key}, index {index} error: {str(e)}")

        if tasks:
            await asyncio.gather(*tasks)


async def process_result(
    cdm_data_bucket: str,
    s3_client: boto3.client,
    file_metadata: CdmFileMetaData,
    source_content_type: ContentType,
    jurisdiction: str,
    prefix: str,
    date_fields: list[str],
    result_model: list[dict],
    index: int,
    filename: str
):
    result_model_valid, result_model_invalid = await filter_result_model(
        result_model, date_fields
    )
    tasks = []
    if result_model_valid:
        logger.info(f"Valid data found for index: {index}: {len(result_model_valid)}")
        tasks.append(
            save_result_model_async(
                s3_client,
                cdm_data_bucket,
                file_metadata,
                source_content_type,
                jurisdiction,
                prefix,
                index,
                result_model_valid,
                target_table=f"stg_{jurisdiction}",
                filename=filename
            )
        )
    else:
        logger.info(f"No valid data found for index: {index}")

    if result_model_invalid:
        logger.info(
            f"Invalid data found for index: {index}: {len(result_model_invalid)}"
        )
        tasks.append(
            save_result_model_async(
                s3_client,
                cdm_data_bucket,
                file_metadata,
                source_content_type,
                jurisdiction,
                prefix,
                index,
                result_model_invalid,
                target_table=f"stg_{jurisdiction}_invalid",
                suffix="invalid",
                filename=filename
            )
        )
    else:
        logger.info(f"No invalid data found for index: {index}")
    await asyncio.gather(*tasks)


async def save_result_model_async(
    s3_client: boto3.client,
    target_bucket: str,
    file_metadata: CdmFileMetaData,
    content_type: str,
    jurisdiction: str,
    prefix: str,
    index: int,
    model: list[dict],
    target_table: str,
    suffix: str | None = None,
    filename: str = None,
):
    """
    asnyc wrapper for save_result_model
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        save_result_model,
        s3_client,
        target_bucket,
        file_metadata,
        content_type,
        jurisdiction,
        prefix,
        index,
        model,
        target_table,
        suffix,
        filename
    )


def save_result_model(
    s3_client: boto3.client,
    target_bucket: str,
    file_metadata: CdmFileMetaData,
    content_type: str,
    jurisdiction: str,
    prefix: str,
    index: int,
    model: list[dict],
    target_table: str,
    suffix: str | None = None,
    filename: str = None,
):
    """
    save result model
    :param s3_client: S3 client
    :param target_bucket: Target bucket
    :param parent_prefix This is used as a part of object key prefix
    :param content_type: Content type
    :param jurisdiction: Jurisdiction
    :param prefix: Prefix
    :param index: Index
    :param model: Model
    :param suffix: Suffix
    """
    if not target_table:
        raise ValueError("target_table is required")
    
    if not model:
        logger.info(f"No invalid data found for index: {index}")
    else:
        timestamp = file_metadata.source_timestamp
        cdm_file_key = get_target_s3_object_key(
            prefix,
            timestamp,
            file_key=os.path.basename(file_metadata.source_name).split(".")[0],
            format=content_type,
            index=index,
            add_file_name_as_prefix=True,
            suffix=suffix,
            filename=filename
        )
        metadata = file_metadata.model_dump(exclude={"hash"})
        metadata.update(
            {"target": f"{target_table}", "jurisdiction": jurisdiction}
        )
        file_arn = save_csv(
            model,
            metadata,
            target_bucket,
            cdm_file_key,
            s3_client,
        )
        del model
        try:
            add_glue_job_task(
                target_bucket,
                os.path.dirname(cdm_file_key), # only need the prefix for glue job
                file_size=-1,
                target_table=target_table
            )
        except ClientError as e:
            logger.error(f"Failed to add glue job task {str(e)}")

        if file_arn:
            logger.info(f"processed {file_arn} successfully")
        else:
            logger.error(f"Error processing file {file_arn}")
            raise ValueError("Error processing file")


async def filter_result_model(
    result_model, date_fields
) -> tuple[list[dict], list[dict]]:
    """
    Filter result model to valid and invalid data based on date fields
    :param result_model: Result model
    :param date_fields: Date fields
    :return: Tuple of valid and invalid data
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: (
            [
                row
                for row in result_model
                if all(
                    is_date_string_valid(
                        row[date_field],
                        format_name="python_date_format",
                        no_data_ok=True,
                    )
                    for date_field in date_fields
                )
            ],
            [
                row
                for row in result_model
                if not all(
                    is_date_string_valid(
                        row[date_field],
                        format_name="python_date_format",
                        no_data_ok=True,
                    )
                    for date_field in date_fields
                )
            ],
        ),
    )


def serialize_json(row: dict, key: str) -> str:
    """
    Serialize JSON
    :param row: row
    :param key: key
    :return: JSON string
    """
    return json.dumps(row[key], ensure_ascii=False)


def save_csv(
    model: list[dict],
    metadata: dict[str, Any],
    cdm_data_bucket: str,
    object_key: str,
    s3_client: boto3.client,
) -> str:
    """
    Save the model to S3 as CSV
    :param file_metadata: dict
    :param cdm_data_bucket: CDM data bucket
    :param model: model
    :return: file ARN
    """
    start = datetime.now()
    csv_buffer = io.StringIO()
    fieldnames = model[0].keys()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in model:
        for key in row:
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime(ISO8901_FORMAT)
            if isinstance(row[key], dict):
                try:
                    row[key] = serialize_json(row, key)
                except TypeError:
                    row[key] = str(row[key])
        writer.writerow(row)

    # create csv file for data bulk loader
    csv_buffer.seek(0)
    s3_client.put_object(
        Bucket=cdm_data_bucket,
        Key=object_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        Metadata=metadata,
        ContentType=ContentType.CSV.value,
    )
    check_time = datetime.now()
    logger.info(
        f"File {object_key} saved in {(check_time - start).total_seconds()} seconds"
    )

    # create compressed csv.bz2 file for glue data loader
    csv_buffer.seek(0)
    target_object_key = f"{object_key}.bz2"
    body = bz2.compress(csv_buffer.getvalue().encode("utf-8"))
    s3_client.put_object(
        Bucket=cdm_data_bucket,
        Key=target_object_key,
        Body=body,
        Metadata=metadata,
        ContentType=ContentType.CSV.value,
        ContentEncoding="bzip2",
    )
    logger.info(
        f"File {target_object_key} saved in {(datetime.now() - check_time).total_seconds()} seconds"
    )
    logger.info(
        f"total save_csv time: {(datetime.now() - start).total_seconds()} seconds"
    )
    return f"{cdm_data_bucket}/{object_key}"

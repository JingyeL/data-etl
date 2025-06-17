import boto3
import base64
from datetime import datetime, timezone
import json
import hashlib
import os
from botocore.exceptions import ClientError
from shared.param_models import FileParam, SFTPData
from shared.constants import ISO8901_FORMAT, SFTP_CONFIG_OBJECT_KEY
from shared.content_type import ContentType


def get_secret(secret_name: str, aws_region: str) -> dict[str, str]:
    """
    Get secret from AWS Secrets Manager
    """

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=aws_region)

    try:
        secret_reponse = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    return json.loads(secret_reponse["SecretString"])


def list_s3_objects(
    s3_client: boto3.client,
    bucket_name: str,
    prefix: str | None = None,
    suffix: list[str] | None = None,
    include_timestamp: bool = False,
) -> list[FileParam]:
    """
    List objects in an S3 bucket.
    :param s3_client: S3 client.
    :param bucket_name: Name of the S3 bucket.
    :param prefix: Prefix to filter objects (optional).
    :param suffix: Suffix to filter objects (optional).
    :param include_timestamp: Include timestamp in the object key (optional).
    :return: List of FileParam.
    """

    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket_name}
    if prefix:
        operation_parameters["Prefix"] = prefix

    object_keys = []
    for page in paginator.paginate(**operation_parameters):
        if "Contents" in page:
            for obj in page["Contents"]:
                file_param = FileParam(
                    # get the prefix from the object key
                    path=os.path.dirname(obj["Key"]),
                    name=os.path.basename(obj["Key"]),
                )
                if suffix:
                    for s in suffix:
                        if obj["Key"].endswith(s):
                            object_keys.append(file_param)
                else:
                    object_keys.append(file_param)

    if not include_timestamp:
        return object_keys

    for obj in object_keys:
        metadata = s3_client.head_object(
            Bucket=bucket_name, Key=os.path.join(obj.path, obj.name)
        ).get("Metadata", {})
        obj.timestamp = metadata["source_timestamp"]
    return object_keys


def files_diff(
    source: list[FileParam], target: list[FileParam], compare_timestamp: bool = False
) -> list[FileParam]:
    """
    Get the difference between two lists of files.
    param source: list of files in the source if compare_timestamp is False.
    Expected to be a list of dictionaries if compare_timestamp is True
    param target: list of files in the target if compare_timestamp is False.
    Expected to be a list of FileParam if compare_timestamp is True
    return a list of files that are in the source but not in the target.
    if compare_timestamp is True, evaluate both base filename and timestamp
    return a list of files that are in the source but not in the target or
    """
    # source = [{'name': "file1", 'timestamp': "2021-01-01T00:00:00"},
    #           {'name': "file2", 'timestamp': "2021-01-01T00:00:00"}]
    # target = [{'name': "file1", 'timestamp': "2021-01-01T00:00:00"},
    #           {'name': "file2", 'timestamp': "2021-01-01T00:00:00"}]
    # return [{'name': "file1", 'timestamp': "2021-01-01T00:00:00"}]
    files = []
    target_dict = {file.name: file for file in target}

    for file in source:
        if file.name not in target_dict:
            files.append(file)
        elif compare_timestamp:
            source_timestamp = None
            target_timestamp = None
            if isinstance(file.timestamp, int):
                source_timestamp = datetime.fromtimestamp(file.timestamp)
            elif isinstance(file.timestamp, str):
                source_timestamp = datetime.strptime(file.timestamp, ISO8901_FORMAT)

            if isinstance(target_dict[file.name].timestamp, int):
                target_timestamp = datetime.fromtimestamp(
                    target_dict[file.name].timestamp
                )
            elif isinstance(target_dict[file.name].timestamp, str):
                target_timestamp = datetime.strptime(
                    target_dict[file.name].timestamp, ISO8901_FORMAT
                )

            if source_timestamp != target_timestamp:
                files.append(file)
            elif source_timestamp is None and target_timestamp is None:
                files.append(file)

    return files


def get_timestamp_string(
    timestamp: datetime | None = None, format: str | None = ISO8901_FORMAT
) -> str:
    """
    Get the timestamp string
    """
    if not timestamp:
        return datetime.now().strftime(format)
    else:
        return datetime.strptime(timestamp, format)


def is_date_string_valid(
    data: dict[str, any],
    format_name: str = "python_date_format",
    no_data_ok: bool = False,
) -> bool:
    """
    Check if the date string is valid
    expecting data:
    {
        "value": "2021-01-01",
        "python_date_format": "%Y-%m-%d"
        "format": "YYYY-MM-DD"
    }

    """
    if not data:
        if no_data_ok:
            return True
        else:
            return False
    # handles encoding - json type is auto encoded to json string
    # if source data is compressed (gzip/bz2)
    if isinstance(data, str):
        data = json.loads(data)
    for _, record in data.items():
        try:
            value = record.get("value")
            format = record.get(format_name)  # e.g. "%m%d%Y", "%d %b %Y"
        except AttributeError:
            return False
        try:
            if (not format and not value) and no_data_ok:
                return True
            elif not value or not format:
                return False
            else:
                datetime.strptime(value, format)
                return True
        except ValueError:
            return False
    return False


def encode_jsonb_fields(data: list[any]) -> any:
    """
    Encode JSONB fields
    accept row data and encode JSONB fields
    """
    encoded_data = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                encoded_data[key] = json.dumps(value)
            else:
                encoded_data[key] = value
    return encoded_data


def get_sftp_meta(
    s3_client: boto3.client, bucket: str, key: str = SFTP_CONFIG_OBJECT_KEY
) -> dict[str, SFTPData]:
    """
    Get all SFTP meta
    """
    source_meta_file = s3_client.get_object(Bucket=bucket, Key=key)

    ftp_meta_dict = json.loads(source_meta_file["Body"].read().decode("utf-8"))
    sftp_data = {}
    for k, v in ftp_meta_dict.items():
        sftp_data[k] = SFTPData(**v)
    return sftp_data


def hash(data: str):
    """
    Hash data
    """
    data_bytes = str(data).encode("utf-8")
    full_hash = hashlib.sha256(data_bytes).digest()
    base64_hash = base64.urlsafe_b64encode(full_hash).decode("utf-8")
    # take even number characters
    return base64_hash[::2]


def get_target_s3_object_key(
    prefix: str,
    timestamp: datetime | None = None,
    file_key: str | None = None,
    format: ContentType | None = ContentType.CSV,
    index: int| None = None,
    add_file_name_as_prefix: bool = False,
    suffix: str | None = None,
    filename: str | None = None,
) -> str:
    """
    Get target file key
    param prefix: S3 prefix
    param timestamp: timestamp
    param file_key: original file key
    param format: file format
    param index: index
    param add_file_name_as_prefix: add file name as prefix
    param suffix: suffix
    param filename: file name
    return target file key
    """
    if not timestamp:
        timestamp = datetime.now(timezone.utc)

    sub_prefix = ""
    if add_file_name_as_prefix:
        sub_prefix = f"{os.path.basename(file_key).split('.')[0] if file_key else 'anon'}/"

    original_filename = os.path.basename(file_key).split('.')[0] if file_key else "anon"
    if filename:
        original_filename = f"{filename.split('.')[0]}"

    index_token = ""
    if index is not None and index >= 0:
        index_token = f"_{index}"

    if suffix:
        return f"{prefix}/{timestamp.year}/{timestamp.month}/{timestamp.day}/{sub_prefix}{original_filename}{index_token}_{suffix}.{format.get_short_name()}"
    else:
        return f"{prefix}/{timestamp.year}/{timestamp.month}/{timestamp.day}/{sub_prefix}{original_filename}{index_token}.{format.get_short_name()}"

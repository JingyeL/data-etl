import pytest
from datetime import datetime
from pydantic import ValidationError
from shared.param_models import IngestWorkload, FileParam
from shared.constants import ISO8901_FORMAT


def test_ingest_workload_valid():
    workload = IngestWorkload(
        jurisdiction="US",
        object_key="some_key",
        periodicity="daily",
        timestamp=1638316800,
        status="NEW",
        ingest_config="some_string"
    )
    assert workload.jurisdiction == "US"
    assert workload.object_key == "some_key"
    assert workload.periodicity == "daily"
    assert workload.timestamp == 1638316800
    assert workload.status == "NEW"
    assert workload.ingest_config == "some_string"


def test_ingest_workload_dict():
    workload = IngestWorkload(
    **{
        "jurisdiction": "US",
        "object_key": "some_key",
        "periodicity": "daily",
        "timestamp": 1638316800,
        "status": "NEW",
        "ingest_config": "some_string"
    }
    )
    assert workload.jurisdiction == "US"
    assert workload.object_key == "some_key"
    assert workload.periodicity == "daily"
    assert workload.timestamp == 1638316800
    assert workload.status == "NEW"
    assert workload.ingest_config == "some_string"


def test_ingest_workload_invalid():
    with pytest.raises(ValidationError):
        IngestWorkload(
            jurisdiction="US", periodicity="daily", status="NEW"
        )


def test_model_dump_dynamo_with_int_timestamp():
    workload = IngestWorkload(
        jurisdiction="US",
        object_key="key1",
        periodicity="daily",
        timestamp=1633036800,
        status="NEW",
        ingest_config="config1",
    )
    expected_output = {
        "jurisdiction": {"S": "US"},
        "object_key": {"S": "key1"},
        "periodicity": {"S": "daily"},
        "timestamp": {"N": "1633036800"},
        "status": {"S": "NEW"},
        "ingest_config": {"S": "config1"},
    }
    assert workload.model_dump_dynamo() == expected_output


def test_model_dump_dynamo_with_str_timestamp():
    timestamp_str = "2021-10-01T00:00:00"
    timestamp_int = int(datetime.strptime(timestamp_str, ISO8901_FORMAT).timestamp())
    workload = IngestWorkload(
        jurisdiction="US",
        object_key="key1",
        periodicity="daily",
        timestamp=timestamp_str,
        status="NEW",
        ingest_config="config1",
    )
    expected_output = {
        "jurisdiction": {"S": "US"},
        "object_key": {"S": "key1"},
        "periodicity": {"S": "daily"},
        "timestamp": {"N": str(timestamp_int)},
        "status": {"S": "NEW"},
        "ingest_config": {"S": "config1"},
    }
    result = workload.model_dump_dynamo()
    assert result == expected_output



def test_model_dump_dynamo_with_default_timestamp():
    workload = IngestWorkload(
        jurisdiction="US",
        object_key="key1",
        periodicity="daily",
        status="NEW",
        ingest_config="config1",
    )
    expected_output = {
        "jurisdiction": {"S": "US"},
        "object_key": {"S": "key1"},
        "periodicity": {"S": "daily"},
        "timestamp": {"N": "0"},
        "status": {"S": "NEW"},
        "ingest_config": {"S": "config1"},
    }
    result = workload.model_dump_dynamo()
    assert result == expected_output


def test_file_param_valid():
    file_param = FileParam(path="/some/path", name="file_name", timestamp=1638316800)
    assert file_param.path == "/some/path"
    assert file_param.name == "file_name"
    assert file_param.timestamp == 1638316800


def test_file_param_invalid():
    with pytest.raises(ValidationError):
        FileParam()
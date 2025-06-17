import boto3
import json
import pytest
from datetime import datetime
from botocore.stub import Stubber
from shared.utils import (
    files_diff, 
    list_s3_objects, 
    get_timestamp_string, 
    is_date_string_valid,
    encode_jsonb_fields
    )
from shared.param_models import FileParam
from pydantic_core._pydantic_core import ValidationError
from shared.utils import get_target_s3_object_key
from shared.content_type import ContentType

def test_empty_file_name():    
    with pytest.raises(ValidationError):
        _ = FileParam(timestamp="2020-01-01T00:00:00")

def test_files_diff_no_difference():
    source = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt"), 
        FileParam(name="file3.txt")]
    target = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt"), 
        FileParam(name="file3.txt")]
    assert files_diff(source, target) == []


def test_files_diff_all_different():
    source = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt"), 
        FileParam(name="file3.txt")]
    target = [
        FileParam(name="file4.txt"), 
        FileParam(name="file5.txt"), 
        FileParam(name="file6.txt")]
    assert files_diff(source, target) == source


def test_files_diff_some_different():
    source = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt"), 
        FileParam(name="file3.txt")]
    target = [
        FileParam(name="file2.txt"), 
        FileParam(name="file4.txt"), 
        FileParam(name="file5.txt")]
    assert files_diff(source, target) == [
        FileParam(name="file1.txt"),
        FileParam(name="file3.txt")
    ]

def test_files_diff_empty_source():
    source = []
    target = [
        FileParam(name="file2.txt"), 
        FileParam(name="file4.txt"), 
        FileParam(name="file5.txt")]
    assert files_diff(source, target) == []

def test_files_diff_timestamp_all_different():
    source = [
        FileParam(name="file1.txt", timestamp="2021-01-01T00:00:00"), 
        FileParam(name="file2.txt", timestamp="2023-01-01T00:00:00"), 
        FileParam(name="file3.txt", timestamp=1609459300)]
    target = [
        FileParam(name="file1.txt", timestamp="2020-01-01T00:00:00"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt")]
    assert files_diff(source, target, compare_timestamp=True) == source

def test_files_diff_timestamp_some_different():
    source = [
        FileParam(name="file1.txt", timestamp="2021-01-01T00:00:00"), 
        FileParam(name="file2.txt", timestamp="2023-01-01T00:00:00"), 
        FileParam(name="file3.txt", timestamp=1609459300)]
    target = [
        FileParam(name="file1.txt", timestamp="2020-01-01T00:00:00"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt", timestamp="2021-01-01T00:01:40")]
    assert files_diff(source, target, compare_timestamp=True) == [
        FileParam(name="file1.txt", timestamp="2021-01-01T00:00:00"), 
        FileParam(name="file2.txt", timestamp="2023-01-01T00:00:00"),
    ]

def test_files_diff_timestamp_with_none():
    source = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt", timestamp=1609459400)]
    target = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt", timestamp=1609459400)]
    assert files_diff(source, target, compare_timestamp=True) == [FileParam(name="file1.txt")]


def test_files_diff_with_timestamp_empty_source():
    source = []
    target = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt", timestamp=1609459400)]
    assert files_diff(source, target, compare_timestamp=True) == []


def test_files_diff_with_timestamp_empty_target():
    source = [
        FileParam(name="file1.txt"), 
        FileParam(name="file2.txt", timestamp=1609459300), 
        FileParam(name="file3.txt", timestamp=1609459400)]
    target = []
    assert files_diff(source, target, compare_timestamp=True) ==source


def test_list_s3_objects_no_prefix():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    response = {
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "file1.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "file2.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "file3.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
        ],
        "Name": "my-bucket",
        "Prefix": "",
        "MaxKeys": 1000,
        "CommonPrefixes": [],
    }
    stubber.add_response("list_objects_v2", response, {"Bucket": "my-bucket"})
    stubber.activate()
    result = list_s3_objects(s3_client, "my-bucket", None, [".txt"])
    assert result == [
        FileParam(path="", name="file1.txt"), 
        FileParam(path="", name="file2.txt"), 
        FileParam(path="", name="file3.txt")]
    stubber.deactivate()


def test_list_s3_objects_with_prefix():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    expected_params = {"Bucket": "my-bucket", "Prefix": "prefix/"}
    response = {
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "prefix/file1.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/file2.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
        ],
        "Name": "my-bucket",
        "Prefix": "",
        "MaxKeys": 1000,
        "CommonPrefixes": [],
    }
    stubber.add_response("list_objects_v2", response, expected_params)
    stubber.activate()

    with Stubber(s3_client):
        result = list_s3_objects(s3_client, "my-bucket", "prefix/", [".txt"])
        assert result == [
            FileParam(path="prefix", name="file1.txt"),
            FileParam(path="prefix", name="file2.txt")
        ]


def test_list_s3_objects_empty_bucket():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    expected_params = {"Bucket": "empty-bucket"}
    response = {"IsTruncated": False, "Contents": []}
    stubber.add_response("list_objects_v2", response, expected_params)
    stubber.activate()

    with Stubber(s3_client):
        result = list_s3_objects(s3_client, "empty-bucket")
        assert result == []
    stubber.deactivate()


def test_list_s3_objects_with_sub_dirs():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    expected_params = {"Bucket": "my-bucket", "Prefix": "prefix/"}
    response = {
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "prefix/file1.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/file2.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/prefix2/",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/prefix2/file3.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "prefix/prefix2/file4.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            },
        ],
        "Name": "my-bucket",
        "Prefix": "",
        "MaxKeys": 1000,
        "CommonPrefixes": [],
    }
    stubber.add_response("list_objects_v2", response, expected_params)
    stubber.activate()

    with Stubber(s3_client):
        result = list_s3_objects(s3_client, "my-bucket", "prefix/", [".txt"])
        assert result == [
            FileParam(path="prefix", name="file1.txt"),
            FileParam(path="prefix", name="file2.txt"),
            FileParam(path="prefix/prefix2", name="file3.txt"),
            FileParam(path="prefix/prefix2", name="file4.txt"),
        ]

def test_list_s3_objects_given_object_key():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    expected_params = {"Bucket": "my-bucket", "Prefix": "prefix/file1.txt"}
    response = {
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "prefix/file1.txt",
                "LastModified": "2021-01-01T00:00:00.000Z",
                "ETag": '"d41d8cd98f00b204e9800998ecf8427e"',
                "Size": 0,
                "StorageClass": "STANDARD",
            }
        ],
        "Name": "my-bucket",
        "Prefix": "",
        "MaxKeys": 1000,
        "CommonPrefixes": [],
    }
    stubber.add_response("list_objects_v2", response, expected_params)
    stubber.activate()

    with Stubber(s3_client):
        result = list_s3_objects(s3_client, "my-bucket", "prefix/file1.txt")
        assert result == [
            FileParam(path="prefix", name="file1.txt")
        ]

def test_get_timestamp_with_valid_string() -> None:
    timestamp_str = "2023-10-05T14:48:00"
    expected_timestamp = datetime(2023, 10, 5, 14, 48, 0)
    assert get_timestamp_string(timestamp_str) == expected_timestamp


def test_get_timestamp_with_invalid_string() -> None:
    timestamp_str = "invalid"
    # assert raises ValueError
    with pytest.raises(ValueError):
        get_timestamp_string(timestamp_str)


def test_get_timestamp_with_default() -> None:
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    timestamp = get_timestamp_string()
    assert timestamp == now

def test_is_date_string_valid_with_valid_date():
        data = {
            "record1": {"value": "2021-01-01", "python_date_format": "%Y-%m-%d"},
            "record2": {"value": "12/31/2021", "python_date_format": "%m/%d/%Y"},
        }
        assert is_date_string_valid(data, "python_date_format") is True

def test_is_date_string_valid_json_string():
    data = {"record1": {"value": "2021-01-01", "python_date_format": "%Y-%m-%d"}}
    assert is_date_string_valid(json.dumps(data), "python_date_format") is True

def test_is_date_string_valid_json_string_2():
    data = {
            "record1": {"value": "2021-01-01", "python_date_format": "%Y-%m-%d"},
            "record2": {"value": "12/31/2021", "python_date_format": "%m/%d/%Y"},
        }
    assert is_date_string_valid(json.dumps(data), "python_date_format") is True

def test_is_date_string_invalid_json_string():
    data = {"record1": "foo"}
    assert not is_date_string_valid(json.dumps(data), "python_date_format")

def test_is_date_string_valid_with_invalid_date_1():
    data = {
        "record1": {"value": "2021-02-31", "python_date_format": "%Y-%m-%d"},
    }
    assert is_date_string_valid(data, "python_date_format") is False


def test_is_date_string_valid_with_invalid_date_2():
    data = {
        "record1": {"value": "2021", "python_date_format": "%m%d%Y"},
    }
    assert is_date_string_valid(data, "python_date_format") is False


def test_is_date_string_valid_with_invalid_date_3():
    data = {
        "record1": {"value": "202101", "python_date_format": "%m%d%Y"},
    }
    assert is_date_string_valid(data, "python_date_format") is False

def test_is_date_string_valid_with_invalid_date_4():
    data = {
        "record1": {"value": "21", "python_date_format": "%m%d%Y"},
    }
    assert is_date_string_valid(data, "python_date_format") is False


def test_is_date_string_valid_with_missing_value():
    data = {
        "record1": {"python_date_format": "%m/%d/%Y"},
    }
    assert is_date_string_valid(data, "python_date_format") is False

def test_is_date_string_valid_with_missing_format():
    data = {
        "record2": {"value": "12/31/2021"},
    }
    assert is_date_string_valid(data, "python_date_format") is False

def test_is_date_string_valid_with_empty_data():
    data = {}
    assert is_date_string_valid(data, "python_date_format", no_data_ok=True) is True

def test_is_date_string_valid_with_none_value_and_format_no_data_ok_1():
    data = {
        "record1": {"value": None, "python_date_format": None},
    }
    assert is_date_string_valid(data, "python_date_format", no_data_ok=True) is True


def test_is_date_string_valid_with_none_value_and_format_no_data_ok_2():
    data = {
        "record1": {},
    }
    assert is_date_string_valid(data, "python_date_format", no_data_ok=True) is True


def test_is_date_string_valid_with_none_value_and_format_no_data_not_ok_1():
    data = {
        "record1": {"value": None, "python_date_format": None},
    }
    assert is_date_string_valid(data, "python_date_format") is False


def test_is_date_string_valid_with_none_value_and_format_no_data_not_ok_2():
    data = {
        "record1": {},
    }
    assert is_date_string_valid(data, "python_date_format") is False


def test_is_date_string_valid_with_none_value():
    data = {
        "record1": {"value": None, "python_date_format": "%Y-%m-%d"},
    }
    assert is_date_string_valid(data, "python_date_format") is False

def test_is_date_string_valid_with_none_format():
    data = {
        "record1": {"value": "2021-01-01", "python_date_format": None},
    }
    assert is_date_string_valid(data, "python_date_format") is False

    
def test_encode_jsonb_fields_with_dict():
    data = {
        "field1": "value1",
        "field2": {"subfield": "subvalue"},
        "field3": ["listitem1", "listitem2"],
        "field4": 123,
    }
    expected = {
        "field1": "value1",
        "field2": json.dumps({"subfield": "subvalue"}),
        "field3": json.dumps(["listitem1", "listitem2"]),
        "field4": 123,
    }
    assert encode_jsonb_fields(data) == expected


def test_encode_jsonb_fields():
    data = {"field1": "value1",
        "field2": {"subfield": "subvalue"},
        "field3": ["listitem1", "listitem2"],
        }
    
    expected = {"field1": "value1",
        "field2": json.dumps({"subfield": "subvalue"}),
        "field3": json.dumps(["listitem1", "listitem2"]),
        }
    assert encode_jsonb_fields(data) == expected


def test_encode_jsonb_fields_with_empty_dict():
    data = {}
    expected = {}
    assert encode_jsonb_fields(data) == expected


def test_encode_jsonb_fields_with_no_jsonb_fields():
    data = {
        "field1": "value1",
        "field2": 123,
        "field3": 45.67,
    }
    expected = {
        "field1": "value1",
        "field2": 123,
        "field3": 45.67,
    }
    assert encode_jsonb_fields(data) == expected


def test_encode_jsonb_fields_with_nested_dict():
    data = {
        "field1": "value1",
        "field2": {"subfield1": {"subsubfield": "subsubvalue"}},
    }
    expected = {
        "field1": "value1",
        "field2": json.dumps({"subfield1": {"subsubfield": "subsubvalue"}}),
    }
    assert encode_jsonb_fields(data) == expected


def test_encode_jsonb_fields_with_nested_list():
    data = {
        "field1": "value1",
        "field2": [["listitem1"], ["listitem2"]],
    }
    expected = {
        "field1": "value1",
        "field2": json.dumps([["listitem1"], ["listitem2"]]),
    }
    assert encode_jsonb_fields(data) == expected

def test_mixed_valid_and_invalid_dates_record():
    data = [{
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-02-30", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    },
    {
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-01-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    }]
    date_fields = ["incorporation_date", "dissolution_date"]
    valid = [row for row in data if all(is_date_string_valid(row[date_field]) for date_field in date_fields)]  
    assert valid == [data[1]]  
    invalid = [row for row in data if not all(is_date_string_valid(row[date_field]) for date_field in date_fields)]
    assert invalid == [data[0]]

def test_valid_date_record():
    data = [{
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-02-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    },
    ]
    date_fields = ["incorporation_date", "dissolution_date"]
    valid = [row for row in data if all(is_date_string_valid(row[date_field]) for date_field in date_fields)]  
    assert valid == data 
    invalid = [row for row in data if not all(is_date_string_valid(row[date_field]) for date_field in date_fields)]
    assert invalid == []
    
def test_valid_date_records():
    data = [{
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-02-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    },
    {
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-01-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    }]
    date_fields = ["incorporation_date", "dissolution_date"]
    valid = [row for row in data if all(is_date_string_valid(row[date_field]) for date_field in date_fields)]  
    assert valid == data 
    invalid = [row for row in data if not all(is_date_string_valid(row[date_field]) for date_field in date_fields)]
    assert invalid == []

def test_invalid_date_records():
    data = [
    {
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    }]
    date_fields = ["incorporation_date", "dissolution_date"]
    valid = [row for row in data if all(is_date_string_valid(row[date_field]) for date_field in date_fields)]  
    assert valid == [] 
    invalid = [row for row in data if not all(is_date_string_valid(row[date_field]) for date_field in date_fields)]
    assert invalid == data

def test_no_invalid_dates_record():
    data = [{
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-02-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    },
    {
        "incorporation_date":{"f1": {"value": "01012021", 
                   "format": "MMDDYYYY", 
                   "python_date_format": "%m%d%Y"
                   }
            },
        "dissolution_date": {"f2": {"value": "2021-01-01", 
                   "format": "YYYY-MM-DD", 
                   "python_date_format": "%Y-%m-%d"
                   }
            },
        "other_field": "foo"
    }]
    date_fields = ["incorporation_date", "dissolution_date"]
    invalid = [row for row in data if not all(is_date_string_valid(row[date_field]) for date_field in date_fields)]  
    assert invalid == [] 

    
def test_list_s3_objects_with_timestamp_no_prefix():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    list_response = {
        "IsTruncated": False,
        "Contents": [
            {"Key": "file1.txt"},
            {"Key": "file2.txt"},
            {"Key": "file3.txt"},
        ],
    }
    head_responses = [
        {"Metadata": {"source_timestamp": "2021-01-01T00:00:00"}},
        {"Metadata": {"source_timestamp": "2021-01-02T00:00:00"}},
        {"Metadata": {"source_timestamp": "2021-01-03T00:00:00"}},
    ]
    stubber.add_response("list_objects_v2", list_response, {"Bucket": "my-bucket"})
    for i, obj in enumerate(list_response["Contents"]):
        stubber.add_response("head_object", head_responses[i], {"Bucket": "my-bucket", "Key": obj["Key"]})
    stubber.activate()
    result = list_s3_objects(s3_client, "my-bucket", prefix=None, suffix=None, include_timestamp=True)
    expected = [
        FileParam(path="", name="file1.txt", timestamp="2021-01-01T00:00:00"),
        FileParam(path="", name="file2.txt", timestamp="2021-01-02T00:00:00"),
        FileParam(path="", name="file3.txt", timestamp="2021-01-03T00:00:00")
    ]
    assert result == expected
    stubber.deactivate()


def test_list_s3_objects_with_timestamp_with_prefix():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    list_response = {
        "IsTruncated": False,
        "Contents": [
            {"Key": "prefix/file1.txt"},
            {"Key": "prefix/file2.txt"},
        ],
    }
    head_responses = [
        {"Metadata": {"source_timestamp": "2021-01-01T00:00:00"}},
        {"Metadata": {"source_timestamp": "2021-01-02T00:00:00"}},
    ]
    stubber.add_response("list_objects_v2", list_response, {"Bucket": "my-bucket", "Prefix": "prefix/"})
    for i, obj in enumerate(list_response["Contents"]):
        stubber.add_response("head_object", head_responses[i], {"Bucket": "my-bucket", "Key": obj["Key"]})
    stubber.activate()

    result = list_s3_objects(s3_client, "my-bucket", "prefix/", include_timestamp=True)
    expected = [
        FileParam(path="prefix", name="file1.txt", timestamp="2021-01-01T00:00:00"),
        FileParam(path="prefix", name="file2.txt", timestamp="2021-01-02T00:00:00"),
    ]
    assert result == expected
    stubber.deactivate()


def test_list_s3_objects_with_timestamp_with_suffix():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    list_response = {
        "IsTruncated": False,
        "Contents": [
            {"Key": "file1.txt"},
            {"Key": "file2.log"},
            {"Key": "file3.txt"},
        ],
    }
    head_responses = [
        {"Metadata": {"source_timestamp": "2021-01-01T00:00:00"}},
        {"Metadata": {"source_timestamp": "2021-01-02T00:00:00"}},
    ]
    stubber.add_response("list_objects_v2", list_response, {"Bucket": "my-bucket"})
    stubber.add_response("head_object", head_responses[0], {"Bucket": "my-bucket", "Key": "file1.txt"})
    stubber.add_response("head_object", head_responses[1], {"Bucket": "my-bucket", "Key": "file3.txt"})
    stubber.activate()

    result = list_s3_objects(s3_client, "my-bucket", None, suffix=[".txt"], include_timestamp=True)
    expected = [
        FileParam(path="", name="file1.txt", timestamp="2021-01-01T00:00:00"),
        FileParam(path="", name="file3.txt", timestamp="2021-01-02T00:00:00"),
    ]
    assert result == expected
    stubber.deactivate()


def test_list_s3_objects_with_timestamp_empty_bucket():
    s3_client = boto3.client("s3")
    stubber = Stubber(s3_client)
    list_response = {"IsTruncated": False, "Contents": []}
    stubber.add_response("list_objects_v2", list_response, {"Bucket": "empty-bucket"})
    stubber.activate()

    result = list_s3_objects(s3_client, "empty-bucket", None, None, include_timestamp=True)
    expected = []
    assert result == expected
    stubber.deactivate()


def test_get_target_s3_object_key_with_original_key():
    prefix = "us_fl"
    timestamp = datetime(2024, 10, 7)
    original_file_key = "path/to/file.csv"
    format = ContentType.CSV
    index = 0
    add_file_name_as_prefix = False

    expected = "us_fl/2024/10/7/file_0.csv"
    result = get_target_s3_object_key(prefix, timestamp, original_file_key, format, index, add_file_name_as_prefix)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_target_s3_object_key_with_add_file_name_as_prefix():
    prefix = "us_fl"
    timestamp = datetime(2024, 10, 7)
    original_file_key = "path/to/file.csv"
    format = ContentType.CSV
    index = 0
    add_file_name_as_prefix = True

    expected = "us_fl/2024/10/7/file/file_0.csv"
    result = get_target_s3_object_key(prefix, timestamp, original_file_key, format, index, add_file_name_as_prefix)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_target_s3_object_key_with_different_format():
    prefix = "us_fl"
    timestamp = datetime(2024, 10, 7)
    original_file_key = "path/to/file.json"
    format = ContentType.Json
    index = 0
    add_file_name_as_prefix = False

    expected = "us_fl/2024/10/7/file_0.json"
    result = get_target_s3_object_key(prefix, timestamp, original_file_key, format, index, add_file_name_as_prefix)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_target_s3_object_key_with_index():
    prefix = "us_fl"
    timestamp = datetime(2024, 10, 7)
    original_file_key = "path/to/file.csv"
    format = ContentType.CSV
    index = 5
    add_file_name_as_prefix = False

    expected = "us_fl/2024/10/7/file_5.csv"
    result = get_target_s3_object_key(prefix, timestamp, original_file_key, format, index, add_file_name_as_prefix)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_target_s3_object_key_without_timestamp():
    prefix = "us_fl"
    original_file_key = "path/to/file.csv"
    format = ContentType.CSV
    index = 1
    add_file_name_as_prefix = False

    result = get_target_s3_object_key(prefix, None, original_file_key, format, index, add_file_name_as_prefix)
    assert result.startswith("us_fl/")
    assert result.endswith("file_1.csv")

def test_get_target_s3_object_key_without_timestamp_2():
    prefix = "us_fl"
    original_file_key = "path/to/file.csv"
    format = ContentType.CSV
    index = 0
    add_file_name_as_prefix = False

    result = get_target_s3_object_key(prefix, None, original_file_key, format, index, add_file_name_as_prefix)
    assert result.startswith("us_fl/")
    assert result.endswith("file_0.csv")

def test_get_target_s3_object_key_without_original_key():
    prefix = "us_fl"
    timestamp = datetime(2024, 10, 7)
    format = ContentType.CSV
    index = None
    add_file_name_as_prefix = False

    expected = "us_fl/2024/10/7/anon.csv"
    result = get_target_s3_object_key(prefix, timestamp, None, format, index, add_file_name_as_prefix)
    assert result == expected, f"Expected {expected}, but got {result}"

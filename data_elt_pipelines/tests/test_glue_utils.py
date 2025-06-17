from unittest.mock import patch, MagicMock
from glue_jobs.glue_utils import get_source_paths
from shared.param_models import FileParam

suffix = [".csv.bz2"]
suffix_invalid = ["_invalid.csv.bz2"]


@patch("glue_jobs.glue_utils.boto3.client")
@patch("glue_jobs.glue_utils.list_s3_objects")
def test_get_source_paths(mock_list_s3_objects, mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    mock_valid_objects = [FileParam(path="valid_path", name="valid_file.csv.bz2")]
    mock_invalid_objects = [
        FileParam(path="invalid_path", name="invalid_file_invalid.csv.bz2")
    ]

    mock_list_s3_objects.side_effect = [mock_valid_objects, mock_invalid_objects]

    object_path = "test-bucket/test-prefix/test-file"
    
    valid_data_files, invalid_data_files = get_source_paths(
        mock_boto3_client, object_path, suffix, suffix_invalid
    )

    assert valid_data_files == ["s3://test-bucket/valid_path/valid_file.csv.bz2"]
    assert invalid_data_files == [
        "s3://test-bucket/invalid_path/invalid_file_invalid.csv.bz2"
    ]


@patch("glue_jobs.glue_utils.boto3.client")
@patch("glue_jobs.glue_utils.list_s3_objects")
def test_get_source_paths_no_files(mock_list_s3_objects, mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    mock_list_s3_objects.side_effect = [[], []]

    object_path = "test-bucket/test-prefix/test-file"
    valid_data_files, invalid_data_files = get_source_paths(
        mock_boto3_client, object_path, suffix, suffix_invalid
    )

    assert valid_data_files == []
    assert invalid_data_files == []


@patch("glue_jobs.glue_utils.boto3.client")
@patch("glue_jobs.glue_utils.list_s3_objects")
def test_get_source_path_multiple_valid_invalid_files(
    mock_list_s3_objects, mock_boto3_client):

    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    mock_valid_objects = [
        FileParam(path="valid_path", name="valid_file.csv.bz2"),
        FileParam(path="valid_path", name="valid_file2.csv.bz2"),
    ]
    mock_invalid_objects = [
        FileParam(path="invalid_path", name="invalid_file_invalid.csv.bz2"),
        FileParam(path="invalid_path", name="invalid_file_invalid2.csv.bz2"),
    ]

    mock_list_s3_objects.side_effect = [mock_valid_objects, mock_invalid_objects]

    object_path = "test-bucket/test-prefix/test-file"
    valid_data_files, invalid_data_files = get_source_paths(
        mock_boto3_client, object_path, suffix, suffix_invalid
    )

    assert valid_data_files == [
        "s3://test-bucket/valid_path/valid_file.csv.bz2",
        "s3://test-bucket/valid_path/valid_file2.csv.bz2",
    ]
    assert invalid_data_files == [
        "s3://test-bucket/invalid_path/invalid_file_invalid.csv.bz2",
        "s3://test-bucket/invalid_path/invalid_file_invalid2.csv.bz2",
    ]
from handlers.fetcher_ftp_download_handler import get_s3_key
from shared.param_models import FileParam


def test_get_s3_key_with_timestamp():
    target_path = "test/path"
    file = FileParam(name="testfile.txt", timestamp="2023-10-01T00:00:00")
    expected_key = "test/path/2023/10/01/testfile.txt"
    result = get_s3_key(target_path, file)
    assert result == expected_key


def test_get_s3_key_without_timestamp():
    target_path = "test/path"
    file = FileParam(name="testfile.txt", timestamp=None)
    result = get_s3_key(target_path, file)
    # Since we cannot predict the exact timestamp, we will check the format
    assert result.startswith("test/path/")
    assert result.endswith("/testfile.txt")


def test_get_s3_key_with_different_timestamp():
    target_path = "another/path"
    file = FileParam(name="anotherfile.txt", timestamp="2022-05-15T00:00:00")
    expected_key = "another/path/2022/05/15/anotherfile.txt"
    result = get_s3_key(target_path, file)
    assert result == expected_key


def test_get_s3_key_with_empty_target_path():
    target_path = ""
    file = FileParam(name="file.txt", timestamp="2021-12-25T00:00:00")
    expected_key = "/2021/12/25/file.txt"
    result = get_s3_key(target_path, file)
    assert result == expected_key
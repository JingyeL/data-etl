import pytest
from pydantic import ValidationError
from shared.param_models import SFTPData, LoginSecret

secret = LoginSecret(username="user", password="pass", host="host", port=22)


def test_sftp_data_valid():
    data = SFTPData(
        jurisdiction="test",
        secret_name="my_secret",
        secret=secret,
        source_path="/source/path",
        target_path="/target/path",
        file_pattern="*.csv",
        file_names="data.csv",
        periodicity="weekly",
        content_type="content type",
        check_timestamp=True
    )
    assert data.jurisdiction == "test"
    assert data.secret_name == "my_secret"
    assert data.secret == secret
    assert data.source_path == "/source/path"
    assert data.target_path == "/target/path"
    assert data.file_pattern == "*.csv"
    assert data.file_names == "data.csv"
    assert data.periodicity == "weekly"
    assert data.content_type == "content type"
    assert data.check_timestamp


def test_sftp_data_default_periodicity():
    data = SFTPData(
        jurisdiction="test",
        secret_name="my_secret",
        secret=secret,
        source_path="/source/path",
        target_path="/target/path",
    )
    assert data.periodicity == "daily"


def test_sftp_data_missing_required_fields():
    with pytest.raises(ValidationError):
        SFTPData()

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from source_ingestion.ftp import Ftp
from shared.param_models import LoginSecret
from shared.constants import ISO8901_FORMAT


@pytest.fixture
def ftp_client():
    secret = LoginSecret(
        host="test_host", port=22, username="test_user", password="test_pass"
    )
    return Ftp(secret)


def test_list_dir_with_timestamp_no_files(ftp_client):
    ftp_client.list_dir = MagicMock(return_value=[])
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        result = ftp_client.list_dir("/test_path", [], extention=None, include_timestamp=True)
    assert result == []


def test_list_dir_with_timestamp_with_files(ftp_client):
    ftp_client.list_dir = MagicMock(return_value=[
        {"name":"file1.txt", "timestamp":datetime.fromtimestamp(1609459300).strftime(ISO8901_FORMAT)},
         {"name": "file2.txt", "timestamp":datetime.fromtimestamp(1609459200).strftime(ISO8901_FORMAT)}])
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.stat.side_effect = [
            MagicMock(st_mtime=1609459300, st_ctime=1609459200),
            MagicMock(st_mtime=1609459200, st_ctime=1609459200),
        ]
        
        result = ftp_client.list_dir("/test_path", [], extention=None, include_timestamp=True)
    expected = [
        {
            "name": "file1.txt",
            "timestamp": datetime.fromtimestamp(1609459300).strftime(ISO8901_FORMAT),
        },
        {
            "name": "file2.txt",
            "timestamp": datetime.fromtimestamp(1609459200).strftime(ISO8901_FORMAT),
        },
    ]
    assert result == expected

def test_list_dir_with_timestamp_mtime_not_set(ftp_client):
    ftp_client.list_dir = MagicMock(return_value=[
        {"name":"file1.txt", "timestamp":datetime.fromtimestamp(1609459200).strftime(ISO8901_FORMAT)},
         {"name": "file2.txt", "timestamp":datetime.fromtimestamp(1609545600).strftime(ISO8901_FORMAT)}])
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.stat.side_effect = [
            MagicMock(st_mtime=None, st_ctime=1609459200),
            MagicMock(st_mtime=None, st_ctime=1609545600),
        ]
        result = ftp_client.list_dir("/test_path", [], extention=None, include_timestamp=True)
    expected = [
        {
            "name": "file1.txt",
            "timestamp": datetime.fromtimestamp(1609459200).strftime(ISO8901_FORMAT),
        },
        {
            "name": "file2.txt",
            "timestamp": datetime.fromtimestamp(1609545600).strftime(ISO8901_FORMAT),
        },
    ]

    assert result == expected


def test_list_dir_with_timestamp_mtime_ctime_not_set(ftp_client):
    ftp_client.list_dir = MagicMock(return_value=[
        {"name":"file1.txt", "timestamp":None},
         {"name": "file2.txt", "timestamp":None}])
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.stat.side_effect = [
            MagicMock(st_mtime=None, st_ctime=None),
            MagicMock(st_mtime=None, st_ctime=None),
        ]
        
        result = ftp_client.list_dir("/test_path", [], extention=None, include_timestamp=True)

        expected = [
            {
                "name": "file1.txt",
                "timestamp": None,
            },
            {
                "name": "file2.txt",
                "timestamp": None,
            },
        ]

        assert result == expected

def test_list_dir_no_extension(ftp_client):
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.listdir.return_value = ["file1.txt", "file2.csv", "file3.txt"]
        result = ftp_client.list_dir("/test_path", file_names=None, extention=None)   
    assert result == ["file1.txt", "file2.csv", "file3.txt"]

def test_list_dir_with_extension(ftp_client):
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.listdir.return_value = ["file1.txt", "file2.csv", "file3.txt"]
        
        result = ftp_client.list_dir("/test_path", file_names=None, extention=".txt")
        assert result == ["file1.txt", "file3.txt"]

def test_list_dir_with_non_matching_extension(ftp_client):
    with patch.object(ftp_client, 'open', return_value=MagicMock()) as mock_open:
        mock_sftp = mock_open.return_value.__enter__.return_value
        mock_sftp.listdir.return_value = ["file1.txt", "file2.csv", "file3.txt"]
    
        result = ftp_client.list_dir("/test_path", file_names=None, extention=".pdf")
        assert result == []

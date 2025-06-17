import os
from datetime import datetime
from contextlib import contextmanager
import logging
import paramiko
from paramiko.sftp_client import SFTPClient
from shared.param_models import LoginSecret
from shared.constants import ISO8901_FORMAT, MULTI_PART_FILE_CHUNK_SIZE as CHUNK_SIZE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Ftp:
    def __init__(self, secret: LoginSecret) -> SFTPClient:
        self.secret = secret
        self.connection = None

    @contextmanager
    def open(self):
        transport = paramiko.Transport((self.secret.host, self.secret.port))
        try:
            transport.connect(
                username=self.secret.username, password=self.secret.password
            )
            self.connection = SFTPClient.from_transport(transport)
            yield self.connection
        finally:
            self.close()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def list_dir(
        self, path: str, 
        file_names: list[str] | None, 
        extention: str | None = None,
        include_timestamp: bool = False
    ) -> list[any]:
        # only list files with the specified extension
 
        with self.open() as sftp:
            files = sftp.listdir(path)
            if not extention:
                if file_names:
                    files = [file for file in files if file in file_names]
            else:
                files = [f for f in files if f.endswith(extention)]
        if not include_timestamp:
            if file_names:
                return [file for file in files if file in file_names]
            else:
                return files
        files_with_timestamp = []
        for file_name in files:
            try:
                timestamp = self.get_file_timestamp(os.path.join(path, file_name)).strftime(ISO8901_FORMAT)
                if file_names:
                    if file_name in file_names:
                        files_with_timestamp.append({"name": file_name, "timestamp": timestamp})
                else:
                    files_with_timestamp.append({"name": file_name, "timestamp": timestamp})
            except Exception as e:
                files_with_timestamp.append({"name": file_name, "timestamp": None})
        
        return files_with_timestamp


    def get_file_timestamp(self, file_path: str) -> datetime:
        with self.open() as sftp:
            file_attributes = sftp.stat(file_path)
            return file_attributes.st_mtime or file_attributes.st_ctime
        
    
    def download_part(self, file_path: str, part_number: int) -> tuple[int, bytes]:
        """
        Download a part of the file
        :param file_path: file path
        :param part_number: part number
        :return: part number and data
        """
        with self.open() as sftp:
            with sftp.file(file_path, "rb") as f:
                f.seek(part_number * CHUNK_SIZE)
                data = f.read(CHUNK_SIZE)
        return part_number, data
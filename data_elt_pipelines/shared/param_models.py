from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from shared.constants import ISO8901_FORMAT


class JobAction(Enum):
    START = "START"
    RETRY = "RETRY"
    RESET = "RESET"
    ADD_WORKLOAD = "ADD_WORKLOAD"

class JobStatus(Enum):
    NEW = "NEW"
    PREPARING = "PREPARING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"

class IngestWorkload(BaseModel):
    jurisdiction: str
    object_key: str
    periodicity: str
    timestamp: Optional[int|str] = 0
    status: str
    ingest_config: str

    def model_dump_dynamo(self):
        _timestamp = 0
        if isinstance(self.timestamp, int):
            _timestamp = self.timestamp
        elif isinstance(self.timestamp, str) and self.timestamp != '0':
             _timestamp = int(datetime.strptime(self.timestamp, ISO8901_FORMAT).timestamp())
        
        return {
            "jurisdiction": {"S": self.jurisdiction},
            "object_key": {"S": self.object_key},
            "periodicity": {"S": self.periodicity},
            "timestamp": {"N": str(_timestamp)},
            "status": {"S":self.status},
            "ingest_config": {"S": self.ingest_config}
        }

class FileParam(BaseModel):
    path: Optional[str] = None
    name: str
    timestamp: Optional[int|str] = None


class LoginSecret(BaseModel):
    host: str = Field(..., description="host")
    port: int = Field(..., description="port")
    username: str = Field(..., description="username")
    password: str = Field(..., description="password")

    class Config:
        title = "LoginSecret"
        description = "SFTP login secret"
        extra = "forbid"


class SFTPData(BaseModel):
    jurisdiction: str = Field(..., description="jurisdiction")
    secret_name: str = Field(..., description="secret name")
    secret: Optional[LoginSecret] = Field(None, description="secret")
    source_path: Optional[str] = Field(..., description="source path at sftp server")
    target_path: Optional[str] = Field(..., description="target path to upload to")
    file_pattern: Optional[str] = Field(
        None, description="file suffix pattern to filter files"
    )
    file_names: Optional[str] = Field(None, description="specific file names to fetch in ',' separated string")
    periodicity: Optional[str] = Field("daily", description="periodicity of the data")
    content_type: Optional[str] = Field(None, description="content type of the data")
    check_timestamp: Optional[bool] = Field(False, description="check timestamp of the file for dowlnoad")
    download_strategy: Optional[str] = Field("multiparts_single_thread", description="for large files (>10mb):multiparts_single_thread|multiparts_multi_threads)")
    max_download_workers: Optional[int] = Field(0, description="max download workers, only valid for multiparts_multi_threads")


class Workload(BaseModel):
    """
    Model representing a Glue job workload configuration.
    """
    bucket: str
    key: str
    size: str
    status: str
    timestamp: Optional[str] = None
    target: Optional[str] = None

    # override the default serializer for the Workload model
    def model_dump(self):
        return {
            "object_key": {"S":f"{self.bucket}/{self.key}"},
            "size": {"S":self.size},
            "status": {"S":self.status},
            "timestamp": {"S":str(self.timestamp) if self.timestamp else ""},
            "target": {"S":self.target if self.target else ""}
        }

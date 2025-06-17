from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import pytz
from shared.constants import ISO8901_FORMAT


class ConfigMetadata(BaseModel):
    """
    Metadata for configuration files
    """
    file_name: str
    version: Optional[str] = None
    last_update: Optional[str] = None
    type: Optional[str] = None
    other_attributes: Optional[dict] = None


class CdmFileMetaData(BaseModel):
    source_name: str = None
    fetched_by: Optional[str] = None
    fetched_at: Optional[datetime] = None
    parsed_by: Optional[str] = None
    parsed_at: Optional[datetime] = None
    cdm_mapping_rules: Optional[str] = None
    cdm_mapped_by: Optional[str] = None
    cdm_mapped_at: Optional[datetime] = None
    fetched_file: Optional[str] = None
    hash: Optional[str] = None
    source_timestamp: Optional[datetime] = None


    def model_dump(self, **kwargs):
        # convert fetched_file to source_name
        self.source_name = self.fetched_file if (self.source_name is None and self.fetched_file) else self.source_name
        data = super().model_dump(**kwargs)
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.strftime(ISO8901_FORMAT)
        try:
            data.pop("fetched_file")
        except KeyError:
            pass
        return data

    @classmethod
    def validate_fetched_at(cls, value):
        if value and value.tzinfo is None:
            value = value.replace(tzinfo=pytz.UTC)
        return value

    @classmethod
    def validate_parsed_at(cls, value):
        if value and value.tzinfo is None:
            value = value.replace(tzinfo=pytz.UTC)
        return value
    
    @classmethod
    def validate_cdm_mapped_at(cls, value):
        if value and value.tzinfo is None:
            value = value.replace(tzinfo=pytz.UTC)
        return value
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v.tzinfo else v.replace(tzinfo=pytz.UTC).isoformat()
        }
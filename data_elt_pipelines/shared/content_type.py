from enum import Enum

class ContentType(Enum):
    """
    Enum class for content
    """
    Json = "application/json"
    Json_lines = "application/jsonlines"
    CSV = "text/csv"
    Parquet = "application/x-parquet"

    def __str__(self):
        return self.value
    
    @classmethod
    def from_string(cls, value: str):
        for member in cls:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(f"{value} is not a valid ContentType")
    
    def get_short_name(self):
        if self == ContentType.Json:
            return "json"
        if self == ContentType.Json_lines:
            return "json_lines"
        if self == ContentType.CSV:
            return "csv"
        if self == ContentType.Parquet:
            return "parquet"
        raise ValueError(f"{self} is not a supported ContentType")

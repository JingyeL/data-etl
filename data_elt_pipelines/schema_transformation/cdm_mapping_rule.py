from enum import Enum
from typing import Optional
from pydantic import Field, BaseModel
from shared.metadata import ConfigMetadata
from shared.content_type import ContentType


class Strategy(Enum):
    """
    default: copy field value if not specified;
    literal: copy literal value;
    regex:   use regex pattern to match source data column names;
    add:     add the cdm_field. If source_field does not exist, add it as new field
    find_regex_parent: find the parent node based on the regex pattern, which is used
    as the key to group the matched data
    """
    # by default, copy the value from source_field
    DEFAULT = ""
    LITERAL = "literal"
    REGEX = "regex"
    ADD = "add"
    # when regex is used in filtering/grouping source data,
    # the matched data (pural) is nested under the label same name as the
    # matching pattern, this label is undersmistic and not declared in the mapping rule
    FIND_REGEX_PARENT = "find_regex_parent"


class ValueType(Enum):
    """
    list: create an empty list[Dict[str, Any]] for cdm_field or cdm_parent
    dict: create a Dict[str, Any] for cdm_field or cdm_parent
    value: cdm_field and source_field are 1 to 1 mapping
    """
    DEFAULT = ""
    LIST = "list"
    DICT = "dict"
    DATE = "date"


class MappingRule(BaseModel):
    source_field: Optional[str] = None
    cdm_parent: Optional[str] = None
    cdm_field: str
    value_type: ValueType = Field(default=ValueType.DEFAULT)
    strategy: Strategy = Field(default=Strategy.DEFAULT)
    pattern_group: Optional[str] | Optional [int] = None
    pattern: Optional[str] = None
    literal_value: Optional[str] = None
    date_format: Optional[str] = None
    python_date_format: Optional[str] = None


class MappingRules(BaseModel):
    meta_data: ConfigMetadata
    rules: dict[str, MappingRule]


def get_mapping_rules_key(jurisdiction: str,
                            content_type: ContentType,
                            version: str|None = None) -> str:
    """
    Get mapping rules key
    :param jurisdiction: jurisdiction code
    :param content_type: content type
    :param version: version of the mapping rules
    :return: mapping rules key
    """
    if not version:
        version = "latest"
    if content_type == ContentType.Json_lines:
        content_type = "jsonlines"
    elif content_type == ContentType.CSV:
        content_type = "csv"
    else:
        content_type = content_type.get_short_name()
    return f"cdm_mapping_rules/{jurisdiction.lower()}/{content_type}/{version}/cdm_mapping_{jurisdiction.lower()}.json"

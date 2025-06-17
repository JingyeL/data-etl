import json
import pytest
from pydantic import ValidationError
from schema_transformation.cdm_mapping_rule import MappingRule, MappingRules, get_mapping_rules_key
from shared.content_type import ContentType

def test_with_no_cdm_field():
    with pytest.raises(ValidationError):
         mapping_rules = {
        "field1": MappingRule(**{"cdm_field": "field1", "value_type": "dict"}),
        "field2": MappingRule(**{"cdm_parent": "field1", "value_type": "list"}),
    }
         

def test_get_mapping_rules_key():
    jurisdiction = "us_fl"
    content_type = ContentType.Json
    version = None
    expected_key = "cdm_mapping_rules/us_fl/json/latest/cdm_mapping_us_fl.json"
    assert get_mapping_rules_key(jurisdiction, content_type, version) == expected_key

    jurisdiction = "FR"
    content_type = ContentType.CSV
    version = "v2"
    expected_key = "cdm_mapping_rules/fr/csv/v2/cdm_mapping_fr.json"
    assert get_mapping_rules_key(jurisdiction, content_type, version) == expected_key

def test_load_mapping_rules():
    #  read mapping rules from file tests/test_data/cdm_mapping_us_fl.json
    mappings_rules_dict = json.load(open("tests/test_data/cdm_mapping_us_fl.json"))
    test_rules = MappingRules(**mappings_rules_dict)
    assert test_rules.meta_data.file_name == "cdm_mapping_us_fl"
    assert test_rules.meta_data.version == "0.1"
    assert test_rules.meta_data.last_update == "2024-10-07"
import csv
import json
import schema_transformation.cdm_mapper as cdm_mapper
from schema_transformation.cdm_mapping_rule import MappingRule, MappingRules
from shared.cdm_company import OrderedEnum
from shared.metadata import CdmFileMetaData


def test_get_dummy_cdm_model():
    expected = {
        "company_number": None,
        "name": None,
        "jurisdiction_code": None,
        "current_status": None,
        "company_type": None,
        "incorporation_date": None,
        "dissolution_date": None,
        "registry_url": None,
        "officers": None,
        "registered_address": None,
        "mailing_address": None,
        "headquarters_address": None,
        "company_identifiers": None,
        "all_attributes": None,
        "previous_names": None,
        "periodicity": None,
        "fetched_by": None,
        "fetched_at": None,
        "parsed_by": None,
        "parsed_at": None,
        "cdm_mapping_rules": None,
        "cdm_mapped_by": None,
        "cdm_mapped_at": None,
        "source_name": None,
        "hash": None,
        "source_timestamp": None,
    }
    result = cdm_mapper.get_dummy_cdm_model(meta_data=meta_data)
    assert sorted(result) == sorted(expected), f"Expected {expected}, but got {result}"


def test_metadata():
    test_meta_data = meta_data.model_copy()
    test_meta_data.source_name = "test_file"
    expected = {
        "fetched_by": None,
        "fetched_at": None,
        "parsed_by": None,
        "parsed_at": None,
        "cdm_mapping_rules": None,
        "cdm_mapped_by": None,
        "cdm_mapped_at": None,
        "source_name": "test_file",
        "hash": None,
        "source_timestamp": None,
    }
    assert (
        test_meta_data.model_dump() == expected
    ), f"Expected {expected}, but got {test_meta_data.model_dump()}"


def test_update_source_name_by_fetched_file():
    test_meta_data = meta_data.model_copy()
    test_meta_data.fetched_file = "test_file"
    expected = {
        "company_number": None,
        "name": None,
        "jurisdiction_code": None,
        "current_status": None,
        "company_type": None,
        "incorporation_date": None,
        "dissolution_date": None,
        "registry_url": None,
        "officers": None,
        "registered_address": None,
        "mailing_address": None,
        "headquarters_address": None,
        "company_identifiers": None,
        "all_attributes": None,
        "previous_names": None,
        "periodicity": None,
        "fetched_by": None,
        "fetched_at": None,
        "parsed_by": None,
        "parsed_at": None,
        "cdm_mapping_rules": None,
        "cdm_mapped_by": None,
        "cdm_mapped_at": None,
        "source_name": "test_file",
        "hash": None,
        "source_timestamp": None,
    }
    result = cdm_mapper.get_dummy_cdm_model(meta_data=test_meta_data)
    assert sorted(result) == sorted(expected), f"Expected {expected}, but got {result}"


def test_get_rules_by_cdm_field():
    field_name = "name"
    expected = [
        MappingRule(
            **{
                "source_field": "RA_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "pattern_group": "RA_",
                "pattern": "(RA_)NAME",
                "strategy": "regex",
            }
        ),
        MappingRule(
            **{
                "source_field": "PRINC(\\d)_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "strategy": "regex",
                "pattern_group": 1,
                "pattern": "PRINC(\\d)_NAME",
            }
        ),
    ]
    result = cdm_mapper.get_rules_by_cdm_field(field_name, mapping_rules.rules)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_find_parent_rule():
    expected = MappingRule(
        **{
            "cdm_field": "other_attributes",
            "cdm_parent": "officers",
            "value_type": "dict",
            "strategy": "find_regex_parent",
            "pattern_group": "RA_",
        }
    )
    rules = [
        expected,
        MappingRule(
            **{
                "cdm_field": "other_attributes",
                "cdm_parent": "officers",
                "value_type": "dict",
                "strategy": "find_regex_parent",
                "pattern_group": 1,
            }
        ),
    ]
    rule = MappingRule(
        **{
            "source_field": "RA_NAME_TYPE",
            "cdm_parent": "other_attributes",
            "strategy": "find_regex_parent",
            "cdm_field": "type",
            "pattern_group": "RA_",
        }
    )
    result = cdm_mapper.find_parent_rule(rules, rule)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_parent_node_top_level():
    expected = {"officers": {}}
    rule = MappingRule(
        **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
    )
    nodes = {"officers": {}}
    result = cdm_mapper.get_parent_node(nodes, rule, mapping_rules)
    assert nodes == expected, f"Expected {expected}, but got {result}"


def test_get_parent_node_add_top_level():
    expected = {"officers": {}}
    rule = MappingRule(
        **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
    )
    nodes = {}
    result = cdm_mapper.get_parent_node(nodes, rule, mapping_rules)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_parent_node_nested():
    nodes = {"officers": {}}
    rule = MappingRule(
        **{
            "source_field": "RA_NAME",
            "cdm_parent": "officers",
            "cdm_field": "name",
            "pattern_group": "RA_",
            "pattern": "(RA_)NAME",
            "strategy": "regex",
        }
    )
    result = cdm_mapper.get_parent_node(nodes, rule, mapping_rules)
    result["RA_"] = {"name": "ABC"}
    expected = {"officers": {"RA_": {"name": "ABC"}}}
    assert expected == nodes, f"Expected {nodes}, but got {result}"


def test_get_regex_node_with_direct_match():
    nodes = {"officers": {"RA_": {"name": "John Doe"}}}
    rule = MappingRule(
        **{
            "source_field": "RA_NAME_TYPE",
            "cdm_field": "type",
            "cdm_parent": "other_attributes",
            "pattern_group": "RA_",
            "strategy": "find_regex_parent",
        }
    )
    result = cdm_mapper.get_regex_node(nodes, rule, mapping_rules.rules)
    expected = {"name": "John Doe", "other_attributes": {}}
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_regex_node_with_pattern_group():
    nodes = {
        "officers": {
            "RA_": {"name": "John Doe", "other_attributes": {"type": "Individual"}}
        }
    }
    rule = MappingRule(
        **{
            "source_field": "RA_NAME_TYPE",
            "cdm_field": "type",
            "cdm_parent": "other_attributes",
            "pattern_group": "RA_",
            "strategy": "regex",
        }
    )
    result = cdm_mapper.get_regex_node(nodes, rule, mapping_rules.rules)
    expected = {"name": "John Doe", "other_attributes": {}}
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_regex_node_with_no_match():
    nodes = {"officers": {"RA_": {"name": "John Doe"}}}
    rule = MappingRule(
        **{
            "source_field": "RA_NAME_TYPE",
            "cdm_field": "type",
            "cdm_parent": "other_attributes",
            "pattern_group": "1",
            "strategy": "regex",
        }
    )
    result = cdm_mapper.get_regex_node(nodes, rule, mapping_rules.rules)
    assert result is None


def test_apply_cdm_model_with_copy_strategy():
    data = {"field1": "value1", "field2": "value2"}
    cdm_model = {"cdm_field1": None, "cdm_field2": None}
    mapping_rules = {
        "field1": MappingRule(**{"source_field": "field1", "cdm_field": "cdm_field1"}),
        "field2": MappingRule(**{"source_field": "field2", "cdm_field": "cdm_field2"}),
    }
    expected = {"cdm_field1": "value1", "cdm_field2": "value2"}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_literal_strategy():
    data = {"field1": "value1", "field2": "value2"}
    cdm_model = {"cdm_field1": "ABC", "cdm_field2": None}
    mapping_rules = {
        "rule1": MappingRule(
            **{
                "cdm_field": "cdm_field1",
                "literal_value": "ABC",
                "strategy": "literal",
            }
        ),
        "field2": MappingRule(**{"source_field": "field2", "cdm_field": "cdm_field2"}),
    }
    expected = {"cdm_field1": "ABC", "cdm_field2": "value2"}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_literal_strategy_in_nested_dict():
    mapping_rules = {
        "officers": MappingRule(
            **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
        ),
        "ra_name": MappingRule(
            **{
                "source_field": "RA_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "pattern_group": "RA_",
                "pattern": "(RA_)NAME",
                "strategy": "regex",
            }
        ),
        "ra_position": MappingRule(
            **{
                "literal_value": "agent",
                "cdm_parent": "officers",
                "cdm_field": "position",
                "strategy": "literal",
                "pattern_group": "RA_",
            }
        ),
    }
    data = {"RA_NAME": "John Doe"}
    expected = {"officers": {"RA_": {"name": "John Doe", "position": "agent"}}}
    cdm_model = {"officers": {}}

    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_add_strategy():
    data = {"field1": "value1", "field3": "value3"}
    cdm_model = {"cdm_field1": {}, "cdm_field2": {}}
    mapping_rules = {
        "field1": MappingRule(
            **{"cdm_field": "cdm_field1", "value_type": "dict", "strategy": "add"}
        ),
        "field2": MappingRule(
            **{"cdm_field": "cdm_field2", "value_type": "dict", "strategy": "add"}
        ),
        "field3": MappingRule(
            **{
                "source_field": "field3",
                "cdm_parent": "cdm_field2",
                "cdm_field": "cdm_field3",
            }
        ),
    }
    expected = {"cdm_field1": {}, "cdm_field2": {"cdm_field3": "value3"}}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_nested_dict():
    data = {"field1": "value1", "field2": "value2", "field3": "value3"}
    cdm_model = {"cdm_field1": {}}
    mapping_rules = {
        "field1": MappingRule(
            **{"cdm_field": "cdm_field1", "value_type": "dict", "strategy": "add"}
        ),
        "field2": MappingRule(
            **{
                "cdm_parent": "cdm_field1",
                "cdm_field": "cdm_field2",
                "value_type": "dict",
                "strategy": "add",
            }
        ),
        "field3": MappingRule(
            **{
                "source_field": "field3",
                "cdm_parent": "cdm_field2",
                "cdm_field": "cdm_field3",
            }
        ),
    }
    expected = {"cdm_field1": {"cdm_field2": {"cdm_field3": "value3"}}}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_empty_data():
    data = {}
    cdm_model = {}
    mapping_rules = {
        "field1": MappingRule(**{"source_field": "field1", "cdm_field": "cdm_field1"}),
        "field2": MappingRule(**{"source_field": "field2", "cdm_field": "cdm_field2"}),
    }
    expected = {}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_no_matching_source_field():
    data = {"field1": "value1", "field2": "value2"}
    cdm_model = {}
    mapping_rules = {
        "field3": MappingRule(**{"source_field": "field3", "cdm_field": "cdm_field3"}),
    }
    expected = {}
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_regex_strategy():
    data = {"PRINC1_ADD_1": "123 Main St", "PRINC1_CITY_1": "New York"}
    cdm_model = {"officers": {}}
    mapping_rules = {
        "officers": MappingRule(
            **{"cdm_field": "officers", "strategy": "add", "value_type": "dict"}
        ),
        "street_address": MappingRule(
            **{
                "source_field": "PRINC1_ADD_1",
                "cdm_parent": "officers",
                "cdm_field": "street_address_1",
                "strategy": "regex",
                "pattern_group": 1,
                "pattern": "PRINC(\\d)_ADD_1",
            }
        ),
        "city": MappingRule(
            **{
                "source_field": "PRINC1_CITY_1",
                "cdm_parent": "officers",
                "cdm_field": "city",
                "strategy": "regex",
                "pattern_group": 1,
                "pattern": "PRINC(\\d)_CITY_1",
            }
        ),
    }
    expected = {
        "officers": {"1": {"street_address_1": "123 Main St", "city": "New York"}}
    }
    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_with_regex_node_and_child_dict():
    mapping_rules = {
        "officers": MappingRule(
            **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
        ),
        "ra_name": MappingRule(
            **{
                "source_field": "RA_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "pattern_group": "RA_",
                "pattern": "(RA_)NAME",
                "strategy": "regex",
            }
        ),
        "ra_name_type_a": MappingRule(
            **{
                "cdm_field": "other_attributes",
                "cdm_parent": "officers",
                "value_type": "dict",
                "strategy": "find_regex_parent",
                "pattern_group": "RA_",
            }
        ),
        "ra_name_type_b": MappingRule(
            **{
                "source_field": "RA_NAME_TYPE",
                "cdm_parent": "other_attributes",
                "strategy": "find_regex_parent",
                "cdm_field": "type",
                "pattern_group": "RA_",
            }
        ),
        "ra_add_1": MappingRule(
            **{
                "source_field": "RA_ADD_1",
                "cdm_parent": "officers",
                "cdm_field": "street_address",
                "pattern_group": "RA_",
                "pattern": "(RA_)ADD_1",
                "strategy": "regex",
            }
        ),
    }
    data = {
        "RA_NAME": "John Doe",
        "RA_NAME_TYPE": "Individual",
        "RA_ADD_1": "123 Main St",
    }
    expected = {
        "officers": {
            "RA_": {
                "name": "John Doe",
                "other_attributes": {
                    "type": "Individual",
                },
                "street_address": "123 Main St",
            }
        }
    }
    cdm_model = {"officers": {}}

    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


######################################################
# find_regex_parent tests
######################################################
def test_apply_cdm_model_find_regex_parent_and_child_dict():
    mapping_rules = {
        "officers": MappingRule(
            **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
        ),
        "ra_name": MappingRule(
            **{
                "source_field": "RA_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "pattern_group": "RA_",
                "pattern": "(RA_)NAME",
                "strategy": "regex",
            }
        ),
        "ra_name_type_a": MappingRule(
            **{
                "cdm_field": "other_attributes",
                "cdm_parent": "officers",
                "value_type": "dict",
                "strategy": "find_regex_parent",
                "pattern_group": "RA_",
            }
        ),
        "ra_name_type_b": MappingRule(
            **{
                "source_field": "RA_NAME_TYPE",
                "cdm_parent": "other_attributes",
                "strategy": "find_regex_parent",
                "cdm_field": "type",
                "pattern_group": "RA_",
            }
        ),
        "ra_add_1": MappingRule(
            **{
                "source_field": "RA_ADD_1",
                "cdm_parent": "officers",
                "cdm_field": "street_address",
                "pattern_group": "RA_",
                "pattern": "(RA_)ADD_1",
                "strategy": "regex",
            }
        ),
    }
    data = {
        "RA_NAME": "John Doe",
        "RA_NAME_TYPE": "Individual",
        "RA_ADD_1": "123 Main St",
    }
    expected = {
        "officers": {
            "RA_": {
                "name": "John Doe",
                "other_attributes": {
                    "type": "Individual",
                },
                "street_address": "123 Main St",
            }
        }
    }
    cdm_model = {"officers": {}}

    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


def test_apply_cdm_model_find_regex_parent_full_register_agent():
    mapping_rules = {
        "officers": MappingRule(
            **{"cdm_field": "officers", "value_type": "dict", "strategy": "add"}
        ),
        "ra_name": MappingRule(
            **{
                "source_field": "RA_NAME",
                "cdm_parent": "officers",
                "cdm_field": "name",
                "pattern_group": "RA_",
                "pattern": "(RA_)NAME",
                "strategy": "regex",
            }
        ),
        "ra_position": MappingRule(
            **{
                "literal_value": "agent",
                "cdm_parent": "officers",
                "cdm_field": "position",
                "strategy": "literal",
                "pattern_group": "RA_",
            }
        ),
        "ra_name_type_a": MappingRule(
            **{
                "cdm_field": "other_attributes",
                "cdm_parent": "officers",
                "value_type": "dict",
                "strategy": "find_regex_parent",
                "pattern_group": "RA_",
            }
        ),
        "ra_name_type_b": MappingRule(
            **{
                "source_field": "RA_NAME_TYPE",
                "cdm_parent": "other_attributes",
                "strategy": "find_regex_parent",
                "cdm_field": "type",
                "pattern_group": "RA_",
            }
        ),
        "ra_add_1": MappingRule(
            **{
                "source_field": "RA_ADD_1",
                "cdm_parent": "officers",
                "cdm_field": "street_address",
                "pattern_group": "RA_",
                "pattern": "(RA_)ADD_1",
                "strategy": "regex",
            }
        ),
        "ra_city": MappingRule(
            **{
                "source_field": "RA_CITY",
                "cdm_parent": "officers",
                "cdm_field": "city",
                "pattern_group": "RA_",
                "pattern": "(RA_)CITY",
                "strategy": "regex",
            }
        ),
        "ra_state": MappingRule(
            **{
                "source_field": "RA_STATE",
                "cdm_parent": "officers",
                "cdm_field": "state",
                "pattern_group": "RA_",
                "pattern": "(RA_)STATE",
                "strategy": "regex",
            }
        ),
        "ra_zip5": MappingRule(
            **{
                "source_field": "RA_ZIP5",
                "cdm_parent": "officers",
                "cdm_field": "zip5",
                "strategy": "regex",
                "pattern_group": "RA_",
                "pattern": "(RA_)ZIP5",
            }
        ),
        "ra_zip4": MappingRule(
            **{
                "source_field": "RA_ZIP4",
                "cdm_parent": "officers",
                "cdm_field": "zip4",
                "pattern_group": "RA_",
                "pattern": "(RA_)ZIP4",
                "strategy": "regex",
            }
        ),
    }
    data = {
        "RA_NAME": "John Doe",
        "RA_NAME_TYPE": "Individual",
        "RA_ADD_1": "123 Main St",
        "RA_CITY": "New York",
        "RA_STATE": "NY",
        "RA_ZIP5": "10001",
        "RA_ZIP4": "1234",
    }
    expected = {
        "officers": {
            "RA_": {
                "name": "John Doe",
                "position": "agent",
                "other_attributes": {
                    "type": "Individual",
                },
                "street_address": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip5": "10001",
                "zip4": "1234",
            }
        }
    }
    cdm_model = {"officers": {}}

    cdm_mapper.apply_cdm_model(data, cdm_model, mapping_rules)
    assert cdm_model == expected, f"Expected {expected}, but got {cdm_model}"


######################################################
# test map whole dataset
######################################################
meta_data = CdmFileMetaData(
    **{
        "fetched_by": None,
        "fetched_at": None,
        "parsed_by": None,
        "parsed_at": None,
        "cdm_mapping_rules": None,
        "cdm_mapped_by": None,
        "cdm_mapped_at": None,
    }
)


def test_map_source_data_type_literal_and_value():
    data = [
        {"field1": "rw1_value1", "field2": "rw1_value2"},
        {"field1": "rw2_value1", "field2": "rw2_value2"},
    ]
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "rule_1": {
                "literal_value": "ABC",
                "strategy": "literal",
                "cdm_field": "cdm_field1",
            },
            "rule_2": {"source_field": "field2", "cdm_field": "cdm_field2"},
        },
    }

    class FieldDef(OrderedEnum):
        FIELD1 = ("cdm_field1", 1)
        FIELD2 = ("cdm_field2", 2)

    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = [
        {"cdm_field1": "ABC", "cdm_field2": "rw1_value2", **meta_data.model_dump()},
        {"cdm_field1": "ABC", "cdm_field2": "rw2_value2", **meta_data.model_dump()},
    ]
    for result, _ in cdm_mapper.schema_transformation(
        data, mapping_rules, meta_data=meta_data, cdm_model_def=FieldDef
    ):
        assert result == expected, f"Expected {expected}, but got {result}"


mapping_rules_dict = {
    "meta_data": {
        "file_name": "test",
        "version": "1.0",
        "last_update": "2021-10-01",
        "type": "test",
    },
    "rules": {
        "officers": {
            "cdm_field": "officers",
            "value_type": "dict",
            "strategy": "add",
        },
        "rule_1": {
            "source_field": "RA_NAME",
            "cdm_parent": "officers",
            "cdm_field": "name",
            "pattern_group": "RA_",
            "pattern": "(RA_)NAME",
            "strategy": "regex",
        },
        "rule_2": {
            "literal_value": "agent",
            "cdm_parent": "officers",
            "cdm_field": "position",
            "strategy": "literal",
            "pattern_group": "RA_",
        },
        "ra_name_type_a": {
            "cdm_field": "other_attributes",
            "cdm_parent": "officers",
            "value_type": "dict",
            "strategy": "find_regex_parent",
            "pattern_group": "RA_",
        },
        "ra_name_type_b": {
            "source_field": "RA_NAME_TYPE",
            "cdm_parent": "other_attributes",
            "strategy": "find_regex_parent",
            "cdm_field": "type",
            "pattern_group": "RA_",
        },
        "princ_name": {
            "source_field": "PRINC(\\d)_NAME",
            "cdm_parent": "officers",
            "cdm_field": "name",
            "strategy": "regex",
            "pattern_group": 1,
            "pattern": "PRINC(\\d)_NAME",
        },
        "princ_title": {
            "source_field": "PRINC(\\d)_TITLE",
            "cdm_parent": "officers",
            "cdm_field": "position",
            "strategy": "regex",
            "pattern_group": 1,
            "pattern": "PRINC(\\d)_TITLE",
        },
        "other_attributes": {
            "cdm_field": "other_attributes",
            "cdm_parent": "officers",
            "value_type": "dict",
            "strategy": "find_regex_parent",
            "pattern_group": 1,
        },
        "princ_type": {
            "source_field": "PRINC(\\d)_TYPE",
            "cdm_parent": "other_attributes",
            "cdm_field": "type",
            "strategy": "find_regex_parent",
            "pattern_group": 1,
            "pattern": "PRINC(\\d)_TYPE",
        },
        "princ_street_address": {
            "source_field": "PRINC(\\d)_ADD_1",
            "cdm_parent": "officers",
            "cdm_field": "street_address",
            "strategy": "regex",
            "pattern_group": 1,
            "pattern": "PRINC(\\d)_ADD_1",
        },
    },
}
mapping_rules = MappingRules(**mapping_rules_dict)


def test_map_source_data_type_regex_child_dict_and_literal():
    data = [
        {
            "field1": "ABC",
            "field2": "rw1_value2",
            "RA_NAME": "Formation Inc",
            "RA_NAME_TYPE": "Organization",
            "PRINC1_NAME": "John Doe",
            "PRINC1_TITLE": "CEO",
            "PRINC1_TYPE": "Individual",
            "PRINC1_ADD_1": "123 Main St",
            "PRINC2_NAME": "Jane Doe",
            "PRINC2_TITLE": "CFO",
            "PRINC2_TYPE": "a person",
            "PRINC2_ADD_1": "456 Main St",
            "PRINC3_NAME": None,
            "PRINC3_TITLE": None,
            "PRINC3_TYPE": None,
            "PRINC3_ADD_1": None,
        },
    ]
    expected = [
        {
            "all_attributes": None,
            "cdm_mapped_at": None,
            "cdm_mapped_by": None,
            "cdm_mapping_rules": None,
            "company_identifiers": None,
            "company_number": None,
            "company_type": None,
            "current_status": None,
            "dissolution_date": None,
            "periodicity": None,
            "fetched_at": None,
            "fetched_by": None,
            "source_name": None,
            "hash": None,
            "source_timestamp": None,
            "headquarters_address": None,
            "incorporation_date": None,
            "jurisdiction_code": None,
            "mailing_address": None,
            "name": None,
            "officers": {
                "RA_": {
                    "name": "Formation Inc",
                    "position": "agent",
                    "other_attributes": {
                        "type": "Organization",
                    },
                },
                "1": {
                    "name": "John Doe",
                    "position": "CEO",
                    "other_attributes": {
                        "type": "Individual",
                    },
                    "street_address": "123 Main St",
                },
                "2": {
                    "name": "Jane Doe",
                    "position": "CFO",
                    "other_attributes": {
                        "type": "a person",
                    },
                    "street_address": "456 Main St",
                },
            },
            "parsed_at": None,
            "parsed_by": None,
            "previous_names": None,
            "registered_address": None,
            "registry_url": None,
        },
    ]
    
    for result, _ in cdm_mapper.schema_transformation(data, mapping_rules, meta_data):
        assert result == expected, f"Expected {expected}, but got {result}"


def test_map_source_child_dict_and_literal():
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "field1": {
                "literal_value": "ABC",
                "strategy": "literal",
                "cdm_field": "cdm_field1",
            },
            "field2": {
                "source_field": "field2",
                "cdm_field": "cdm_field2",
                "value_type": "dict",
            },
            "field3": {
                "source_field": "field3",
                "cdm_parent": "cdm_field2",
                "cdm_field": "cdm_field3",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    data = [
        {"field3": "rw1_value3"},
        {"field3": "rw2_value3"},
    ]
    expected = [
        {
            "cdm_field1": "ABC",
            "cdm_field2": {"cdm_field3": "rw1_value3"},
            **meta_data.model_dump(),
        },
        {
            "cdm_field1": "ABC",
            "cdm_field2": {"cdm_field3": "rw2_value3"},
            **meta_data.model_dump(),
        },
    ]

    class TestModel(OrderedEnum):
        FIELD1 = ("cdm_field1", 1)
        FIELD2 = ("cdm_field2", 2)

    for result, _ in cdm_mapper.schema_transformation(
        data, mapping_rules, meta_data, TestModel
    ):
        assert result == expected, f"Expected {expected}, but got {result}"


def test_map_real_data():
    # read data from csv in a list of dictionaries
    file = "tests/test_data/us_fl_source_small.csv"
    mapping_rules_dict = json.load(open("tests/test_data/cdm_mapping_us_fl.json"))
    mapping_rules = MappingRules(**mapping_rules_dict)
    data = []
    with open(file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    expected = json.load(open("tests/test_data/expected_cdm_us_fl_small.json"))
    for result, _ in cdm_mapper.schema_transformation(data, mapping_rules, meta_data):
        assert len(result) == len(
            expected
        ), f"Expected {len(expected)} records, but got {len(result)}"


def test_map_incorpration_date():
    data = [
        {
            "COR_DISS_DATE": None,
            "COR_FILE_DATE": "01022024",
        },
        {
            "COR_DISS_DATE": None,
            "COR_FILE_DATE": "01022022",
        },
    ]
    expected = [
        {
            "incorporation_date": {
                "cor_file_date": {
                    "value": "01022024",
                    "format": "MMDDYYYY",
                    "python_date_format": "%m%d%Y",
                }
            },
            "dissolution_date": None,
            **meta_data.model_dump(),
        },
        {
            "incorporation_date": {
                "cor_file_date": {
                    "value": "01022022",
                    "format": "MMDDYYYY",
                    "python_date_format": "%m%d%Y",
                }
            },
            "dissolution_date": None,
            **meta_data.model_dump(),
        },
    ]
    mapping_rules_dict = mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "incorporation_dates": {
                "cdm_field": "incorporation_date",
                "value_type": "dict",
                "strategy": "add",
            },
            "incorporation_date_entity": {
                "cdm_parent": "incorporation_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "cor_file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "dissolution_dates": {
                "cdm_field": "dissolution_date",
                "value_type": "dict",
                "strategy": "add",
            },
            "dissolution_date_entity": {
                "cdm_parent": "dissolution_date",
                "source_field": "COR_DISS_DATE",
                "cdm_field": "cor_diss_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
        },
    }

    class TestModel(OrderedEnum):
        incorporation_date = ("incorporation_date", 1)
        dissolution_date = ("dissolution_date", 2)

    mapping_rules = MappingRules(**mapping_rules_dict)
    for result, _ in cdm_mapper.schema_transformation(data, mapping_rules, meta_data, TestModel):
        assert result == expected, f"Expected {expected}, but got {result}"


def test_map_nested_literal_field():
    data = [
        {
            "COR_FEI_NUMBER": "value1",
            "STATE_COUNTRY": "FL",
        },
        {
            "COR_FEI_NUMBER": "value2",
            "STATE_COUNTRY": "IE",
        },
    ]
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "all_attributes": {
                "cdm_field": "all_attributes",
                "value_type": "dict",
                "strategy": "add",
            },
            "jurisdiction_of_origin": {
                "source_field": "STATE_COUNTRY",
                "cdm_parent": "all_attributes",
                "cdm_field": "jurisdiction_of_origin",
            },
            "identifier_system_code": {
                "literal_value": "us_fein",
                "cdm_parent": "all_attributes",
                "cdm_field": "identifier_system_code",
                "strategy": "literal",
            },
            "company_identifiers": {
                "cdm_field": "company_identifiers",
                "value_type": "dict",
                "strategy": "add",
            },
            "company_identifier_uid": {
                "source_field": "COR_FEI_NUMBER",
                "cdm_parent": "company_identifiers",
                "cdm_field": "uid",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = [
        {
            "all_attributes": {
                "identifier_system_code": "us_fein",
                "jurisdiction_of_origin": "FL",
            },
            "company_identifiers": {"uid": "value1"},
            **meta_data.model_dump(),
        },
        {
            "all_attributes": {
                "identifier_system_code": "us_fein",
                "jurisdiction_of_origin": "IE",
            },
            "company_identifiers": {"uid": "value2"},
            **meta_data.model_dump(),
        },
    ]

    class TestModel(OrderedEnum):
        all_attributes = ("all_attributes", 1)
        company_identifiers = ("company_identifiers", 2)

    for result, _ in cdm_mapper.schema_transformation(data, mapping_rules, meta_data, TestModel):
        assert result == expected, f"Expected {expected}, but got {result}"


def test_map_nested_multiple_literal_fields():
    data = [
        {
            "COR_FEI_NUMBER": "value1",
        },
        {
            "COR_FEI_NUMBER": "value2",
        },
    ]
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "all_attributes": {
                "cdm_field": "all_attributes",
                "value_type": "dict",
                "strategy": "add",
            },
            "jurisdiction_of_origin": {
                "source_field": "STATE_COUNTRY",
                "cdm_parent": "all_attributes",
                "cdm_field": "jurisdiction_of_origin",
                "literal_value": "FL",
                "strategy": "literal",
            },
            "identifier_system_code": {
                "literal_value": "us_fein",
                "cdm_parent": "all_attributes",
                "cdm_field": "identifier_system_code",
                "strategy": "literal",
            },
            "company_identifiers": {
                "cdm_field": "company_identifiers",
                "value_type": "dict",
                "strategy": "add",
            },
            "company_identifier_uid": {
                "source_field": "COR_FEI_NUMBER",
                "cdm_parent": "company_identifiers",
                "cdm_field": "uid",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = [
        {
            "all_attributes": {
                "identifier_system_code": "us_fein",
                "jurisdiction_of_origin": "FL",
            },
            "company_identifiers": {"uid": "value1"},
            **meta_data.model_dump(),
        },
        {
            "all_attributes": {
                "identifier_system_code": "us_fein",
                "jurisdiction_of_origin": "FL",
            },
            "company_identifiers": {"uid": "value2"},
            **meta_data.model_dump(),
        },
    ]

    class TestModel(OrderedEnum):
        all_attributes = ("all_attributes", 1)
        company_identifiers = ("company_identifiers", 2)

    for result, _ in cdm_mapper.schema_transformation(data, mapping_rules, meta_data, TestModel):
        assert result == expected, f"Expected {expected}, but got {result}"


def test_get_date_fields_with_date_fields():
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "cor_file_date": {
                "cdm_parent": "incorporation_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "cor_diss_date": {
                "cdm_parent": "dissolution_date",
                "source_field": "COR_DISS_DATE",
                "cdm_field": "diss_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = ["incorporation_date", "dissolution_date"]
    result = cdm_mapper.get_date_fields(mapping_rules)
    assert sorted(result) == sorted(expected), f"Expected {expected}, but got {result}"


def test_get_date_fields_with_no_date_fields():
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "field1": {
                "cdm_parent": "parent1",
                "source_field": "FIELD1",
                "cdm_field": "field1",
                "value_type": "dict",
                "strategy": "add",
            },
            "field2": {
                "cdm_parent": "parent2",
                "source_field": "FIELD2",
                "cdm_field": "field2",
                "value_type": "dict",
                "strategy": "add",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = []
    result = cdm_mapper.get_date_fields(mapping_rules)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_date_fields_with_mixed_fields():
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "cor_file_date": {
                "cdm_parent": "incorporation_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "field1": {
                "cdm_parent": "parent1",
                "source_field": "FIELD1",
                "cdm_field": "field1",
                "value_type": "dict",
                "strategy": "add",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = ["incorporation_date"]
    result = cdm_mapper.get_date_fields(mapping_rules)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_get_date_fields_with_duplicated_fields():
    mapping_rules_dict = {
        "meta_data": {
            "file_name": "test",
            "version": "1.0",
            "last_update": "2021-10-01",
            "type": "test",
        },
        "rules": {
            "cor_file_date": {
                "cdm_parent": "incorporation_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "cor_diss_date": {
                "cdm_parent": "dissolution_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "cor_register_date": {
                "cdm_parent": "incorporation_date",
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
            "cor_date": {
                "source_field": "COR_FILE_DATE",
                "cdm_field": "file_date",
                "value_type": "date",
                "date_format": "MMDDYYYY",
                "python_date_format": "%m%d%Y",
                "strategy": "add",
            },
        },
    }
    mapping_rules = MappingRules(**mapping_rules_dict)
    expected = ["incorporation_date", "dissolution_date", "file_date"]
    result = cdm_mapper.get_date_fields(mapping_rules)
    assert sorted(result) == sorted(expected), f"Expected {expected}, but got {result}"

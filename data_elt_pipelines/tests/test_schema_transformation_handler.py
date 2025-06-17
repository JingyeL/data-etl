from datetime import datetime
import schema_transformation.cdm_mapper as cdm_mapper
from shared.metadata import CdmFileMetaData
from shared.cdm_company import OrderedEnum


metadata = CdmFileMetaData(
    **{
        "fetched_by": "test_user",
        "fetched_at": datetime.fromisoformat("2024-10-12T01:00:00"),
        "parser_by": "test_parser",
        "parsed_at": datetime.fromisoformat("2024-10-12T01:00:00"),
        "cdm_mapping_rules": "test_rules",
        "cdm_mapped_by": "local_test_schema_transform",
        "cdm_mapped_at": datetime.fromisoformat("2024-10-12T01:00:00"),
    }
)


def test_set_metadata_with_empty_cdm_model():
    cdm_model = {}
    expected = {**metadata.model_dump()}
    result = cdm_mapper.set_metadata(cdm_model, metadata)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_set_metadata_with_non_empty_cdm_model():
    cdm_model = {"company_number": "123", "name": "Test Company"}

    expected = {
        "company_number": "123",
        "name": "Test Company",
        **metadata.model_dump(),
    }
    result = cdm_mapper.set_metadata(cdm_model, metadata)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_set_metadata_with_dummy_cdm_model():
    class TestModel(OrderedEnum):
        COMPANY_NUMBER = ("company_number", 1)
        NAME = ("name", 2)

    cdm_model = cdm_mapper.get_dummy_cdm_model(meta_data=metadata, model_def=TestModel)
    expected = {"company_number": None, "name": None, **metadata.model_dump()}
    result = cdm_mapper.set_metadata(cdm_model, metadata)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_set_metadata_with_partial_metadata():
    class TestModel(OrderedEnum):
        COMPANY_NUMBER = ("company_number", 1)
        NAME = ("name", 2)

    cdm_model = cdm_mapper.get_dummy_cdm_model(meta_data=metadata, model_def=TestModel)
    cdm_model["company_number"] = "123"
    cdm_model["name"] = "Test Company"
    cdm_model["cdm_mapped_by"] = "test_user"
    expected = {
        "company_number": "123",
        "name": "Test Company",
        **metadata.model_dump(),
    }
    expected["cdm_mapped_by"] = "test_user"
    result = cdm_mapper.set_metadata(cdm_model, metadata)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_is_metadata_set_with_metadata():
    cdm_model = {"field1": "value1", "field2": "value2", **metadata.model_dump()}
    assert cdm_mapper.is_metadata_set(cdm_model, metadata)


def test_is_metadata_set_without_metadata():
    cdm_model = {"field1": "value1", "field2": "value2"}
    assert not cdm_mapper.is_metadata_set(cdm_model, metadata)


def test_is_metadata_set_with_partial_metadata():
    cdm_model = {"field1": "value1", "field2": "value2", "cdm_mapped_by": "test_user"}
    assert not cdm_mapper.is_metadata_set(cdm_model, metadata)


def test_is_metadata_set_with_empty_model():
    cdm_model = {}
    assert not cdm_mapper.is_metadata_set(cdm_model, metadata)

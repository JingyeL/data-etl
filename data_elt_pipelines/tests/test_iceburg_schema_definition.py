from glue_jobs.schema_definition import cdm_schema, get_timestamp_fields

def test_get_timestamp_fields():
    expected = ["fetched_at", "parsed_at", "cdm_mapped_at"]
    result = get_timestamp_fields(cdm_schema)
    assert result == expected, f"Expected {expected}, but got {result}"

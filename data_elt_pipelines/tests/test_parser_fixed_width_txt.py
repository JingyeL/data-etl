import csv
import json
from io import StringIO
from raw_parsers.fixed_width_txt import parse, normalize_string, parse_chunks


def test_parser_with_valid_data():
    input_data = StringIO("L24000407310ABCD LLCXY\nL24000407320APPLE INYX")
    mapping = {
        "meta_data": {"length": 22},
        "fields": [
            {"name": "COR_NUMBER", "start": 1, "end": 12, "length": 12},
            {"name": "COR_NAME", "start": 13, "end": 20, "length": 8},
            {"name": "COR_STATUS", "start": 21, "end": 22, "length": 2},
        ],
    }
    expected_output = StringIO(
        "COR_NUMBER,COR_NAME,COR_STATUS,hash\r\n"
        "L24000407310,ABCD LLC,XY,SPTscM6htn9_ScYpM6BpjU\r\n"
        "L24000407320,APPLE IN,YX,VLiUOCZssSq2-br5HPpPr0\r\n"
    )
    output = parse(input_data, mapping)
    assert output.getvalue() == expected_output.getvalue()


def test_parser_with_leading_whitespace():
    input_data = StringIO(" L24000407310ABCD LLCX\n L24000407320APPLE INY")
    mapping = {
        "meta_data": {"length": 22},
        "fields": [
            {"name": "COR_NUMBER", "start": 2, "end": 13, "length": 12},
            {"name": "COR_NAME", "start": 14, "end": 21, "length": 8},
            {"name": "COR_STATUS", "start": 22, "end": 23, "length": 2},
        ],
    }

    expected_output = StringIO(
        "COR_NUMBER,COR_NAME,COR_STATUS,hash\r\n"
        "L24000407310,ABCD LLC,X,e28B40-FROgKO8SdZVc3Es\r\n"
        "L24000407320,APPLE IN,Y,g9jTEXNqRj5-brHKmuHSm0\r\n"
    )

    output = parse(input_data, mapping)
    assert output.getvalue() == expected_output.getvalue()


def test_parser_with_invalid_data_unmatch_length():
    input_data = StringIO("L24000407310ABCD LLCXY\nL24000407320APPLE IN")
    mapping = {
        "meta_data": {"length": 22},
        "fields": [
            {"name": "COR_NUMBER", "start": 1, "end": 12, "length": 12},
            {"name": "COR_NAME", "start": 13, "end": 20, "length": 8},
            {"name": "COR_STATUS", "start": 21, "end": 22, "length": 2},
        ],
    }

    expected_output = StringIO(
        "COR_NUMBER,COR_NAME,COR_STATUS,hash\r\n"
        "L24000407310,ABCD LLC,XY,SPTscM6htn9_ScYpM6BpjU\r\n"
        "L24000407320,APPLE IN,,IW6727l9KTns4XAeZVGyJI\r\n"
    )

    output = parse(input_data, mapping)
    assert output.getvalue() == expected_output.getvalue()


def test_with_real_data():
    input_data_file = "tests/test_data/us_fl_fixed_width_small.txt"
    field_def = "../config/fixed_width_field_def/us_fl.json"
    expected_file = "tests/test_data/us_fl_fixed_width_small_converted.csv"

    expected = StringIO()
    with open(expected_file, "r") as f:
        expected.write(f.read())
    expected.seek(0)

    input_data = StringIO()
    with open(input_data_file, "r") as f:
        input_data.write(f.read())
    input_data.seek(0)

    with open(field_def, "r") as f:
        mapping = json.load(f)

    output = parse(input_data, mapping, add_hash=False)

    normalized_output = normalize_string(output.getvalue())
    normalized_expected = normalize_string(expected.getvalue())
    assert normalized_output.rstrip("\n") == normalized_expected


def test_parse_chunks_single_chunk():
    input_data = StringIO("1234567890\n0987654321")
    mapping = {
        "fields": [
            {"name": "field1", "start": 1, "end": 5},
            {"name": "field2", "start": 6, "end": 10},
        ]
    }
    chunks = list(parse_chunks(input_data, mapping, chunk_size=2))
    assert len(chunks) == 1
    csv_data, index = chunks[0]
    assert index == 0
    assert (
        csv_data.getvalue()
        == "field1,field2,hash\r\n12345,67890,lCNYoUWhll5-5Z2nbxhF-M\r\n09876,54321,nCamtIqAJIg6MA3ZpRoKfc\r\n"
    )


def test_parse_chunks_multiple_chunks():
    input_data = StringIO("1234567890\n0987654321\n1122334455\n5566778899")
    mapping = {
        "fields": [
            {"name": "field1", "start": 1, "end": 5},
            {"name": "field2", "start": 6, "end": 10},
        ]
    }

    chunks = list(parse_chunks(input_data, mapping, chunk_size=2))
    assert len(chunks) == 2
    csv_buffer_1, index = chunks[0]
    assert index == 0
    csv_buffer_2, index = chunks[1]
    assert (
        csv_buffer_1.getvalue()
        == "field1,field2,hash\r\n12345,67890,lCNYoUWhll5-5Z2nbxhF-M\r\n09876,54321,nCamtIqAJIg6MA3ZpRoKfc\r\n"
    )
    assert index == 1
    assert (
        csv_buffer_2.getvalue()
        == "field1,field2,hash\r\n11223,34455,5iElFTiurdL1CAh1dfKBjw\r\n55667,78899,ysUUUjilK9xqtvWET-2JVA\r\n"
    )


def test_parse_chunks_single_chunk_4():
    input_data = StringIO("1234567890\n0987654321\n1122334455\n5566778899")
    mapping = {
        "fields": [
            {"name": "field1", "start": 1, "end": 5},
            {"name": "field2", "start": 6, "end": 10},
        ]
    }

    chunks = list(parse_chunks(input_data, mapping, chunk_size=4))
    assert len(chunks) == 1
    csv_buffer_1, index = chunks[0]
    csv_buffer_1.seek(0)

    assert index == 0
    assert (
        csv_buffer_1.getvalue()
        == "field1,field2,hash\r\n12345,67890,lCNYoUWhll5-5Z2nbxhF-M\r\n09876,54321,nCamtIqAJIg6MA3ZpRoKfc\r\n11223,34455,5iElFTiurdL1CAh1dfKBjw\r\n55667,78899,ysUUUjilK9xqtvWET-2JVA\r\n"
    )


def test_parse_chunks_no_hash():
    input_data = StringIO("1234567890\n0987654321")
    mapping = {
        "fields": [
            {"name": "field1", "start": 1, "end": 5},
            {"name": "field2", "start": 6, "end": 10},
        ]
    }
    chunks = list(parse_chunks(input_data, mapping, add_hash=False, chunk_size=2))
    assert len(chunks) == 1
    csv_data, index = chunks[0]
    assert index == 0
    assert csv_data.getvalue() == "field1,field2\r\n12345,67890\r\n09876,54321\r\n"

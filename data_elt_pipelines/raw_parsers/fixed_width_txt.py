import csv
import logging
from io import StringIO
from shared.utils import hash
from shared.constants import DEFAULT_DATA_CHUNK_SIZE
from typing import Generator


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def normalize_string(content):
    return content.replace('"', "").replace("\r\n", "\n").replace("\x00", "")


def parse_chunks(
    input: StringIO,
    mapping: dict[str, any],
    add_hash: bool = True,
    chunk_size: int = DEFAULT_DATA_CHUNK_SIZE,
) -> Generator[tuple[StringIO, int], None, None]:
    """
    # Parse a fixed width file using a mapping file, chunk it to the chunk size 
    # and return a csv StringIO object and the index of the chunk
    """
    fields = mapping["fields"]
    field_names = [field["name"] for field in fields]
    field_positions = [(field["start"] - 1, field["end"]) for field in fields]
    data = input.getvalue().splitlines()

    # Iterate over the data and chunk it, get the index of the chunk
    chunks = [
        data[i : i + chunk_size] for i in range(0, len(data), chunk_size)
    ]

    for index, chunk in enumerate(chunks):
        csv_data = StringIO()
        csv_writer = csv.writer(csv_data)
        if add_hash:
            csv_writer.writerow(field_names + ["hash"])
        else:
            csv_writer.writerow(field_names)
        for line in chunk:
            line = normalize_string(line)
            row = [line[start:end].strip() for start, end in field_positions]
            if add_hash:
                row_hash = hash(tuple(row))
                row.append(row_hash)
            csv_writer.writerow(row)
        csv_data.seek(0)
        yield csv_data, index


def parse(input: StringIO, mapping: dict[str, any], add_hash: bool = True) -> StringIO:
    """
    Parse a fixed width file using a mapping file and return a csv StringIO object
    :param input_file: The input fixed width file
    :param mapping: The mapping dict
    :param add_hash: Add a hash column to the output
    """

    csv_data = StringIO()
    fields = mapping["fields"]
    field_names = [field["name"] for field in fields]
    field_positions = [(field["start"] - 1, field["end"]) for field in fields]

    data = input.getvalue().splitlines()
    csv_writer = csv.writer(csv_data)
    if add_hash:
        csv_writer.writerow(field_names + ["hash"])
    else:
        csv_writer.writerow(field_names)
    for line in data:
        line = normalize_string(line)
        row = [line[start:end].strip() for start, end in field_positions]
        if add_hash:
            row_hash = hash(tuple(row))
            row.append(row_hash)
        csv_writer.writerow(row)
    csv_data.seek(0)
    return csv_data

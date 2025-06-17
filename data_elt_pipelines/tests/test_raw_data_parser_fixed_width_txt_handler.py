import datetime
import handlers.fixed_width_text_handler as handler
from handlers.fixed_width_text_handler import ContentType


def test_get_targte_file_key_default_csv():
    raw_file_key = "us_fl/2021/01/01/abc.txt"
    jurisdiction = "us_fl"
    timestamp = datetime.date(2024, 10, 21)
    target_file_key = handler.get_target_s3_object_key(
        jurisdiction, timestamp, raw_file_key
    )
    assert target_file_key == "us_fl/2024/10/21/abc.csv"


def test_get_targte_file_key_default():
    raw_file_key = "us_fl/2021/01/01/abc.txt"
    jurisdiction = "us_fl"
    timestamp = datetime.date(2024, 10, 21)

    target_file_key = handler.get_target_s3_object_key(
        jurisdiction, timestamp, raw_file_key, format=ContentType.Parquet
    )
    assert target_file_key == "us_fl/2024/10/21/abc.parquet"

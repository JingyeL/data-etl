import boto3
import os
import logging
import csv
from io import StringIO

logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    file_path = (
        "us_fl_historical/2024/10/8/us_fl_historical_2024_10_08_source_cordata0.csv"
    )
    bucket = "data-pipeline-tmpdata-poc-data-source"
    s3_client = boto3.client("s3", region_name=os.getenv("region"))
    file_obj = s3_client.get_object(Bucket=bucket, Key=file_path)
    file_content = file_obj["Body"].read().decode("utf-8")
    source_data = list(csv.DictReader(file_content.splitlines()))
    metadata = file_obj.get("Metadata", {})
    field_names = list(source_data[0].keys())
    field_names = source_data[0].keys()
    logger.info(f"Field names: {field_names}")

    chunks = [1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000]

    for chunk in chunks:
        csv_data = StringIO()
        rows = source_data[:chunk]
        # save the first chunk to a new file in the same bucket, with same prefix and suffix
        new_file_path = file_path.replace(".csv", f"_{str(chunk)}.csv")
        csv_writer = csv.writer(csv_data)
        csv_writer.writerow(field_names)
        csv_writer.writerows(rows)
        csv_data.seek(0)
        s3_client.put_object(
            Bucket=bucket,
            Key=new_file_path,
            Body=csv_data.getvalue(),
            Metadata=metadata,
            ContentType="text/csv",
        )
        logger.info(f"Uploaded {new_file_path} with {chunk} rows")

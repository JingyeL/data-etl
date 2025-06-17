import boto3
import logging
import os

from typing import Optional
from botocore.exceptions import ClientError
from datetime import datetime
from shared.param_models import IngestWorkload
from shared.constants import ISO8901_FORMAT, DEFAULT_REGION
from shared.param_models import JobStatus, Workload
from shared.utils import get_timestamp_string


logger = logging.getLogger()


class DynamoTable:
    def __init__(self, table_name: str, dynamodb: boto3.client):
        self.table = table_name
        self.dynamodb = dynamodb

    def get_new_items(
        self,
        items: list = [],
        start_key: Optional[dict[str, any]] = None,
        limit: int = 1,
    ) -> list[dict[str, any]]:
        """
        Get new items from the DynamoDB table
        :param items: list of items
        :param start_key: start key
        :param limit: limit of items to get
        :param filter_object_prefix: optional prefix to filter object keys
        :param status: optional status to filter items
        :return: list of items

                "object_key",
                "stage",
                "status"
        """
        if not start_key:
            response = self.dynamodb.scan(
                TableName=self.table,
                FilterExpression="attribute_not_exists(#status) OR #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": {"S": "NEW"}},
                Limit=limit,
            )
        else:
            response = self.dynamodb.scan(
                TableName=self.table,
                FilterExpression="attribute_not_exists(#status) OR #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": {"S": "NEW"}},
                ExclusiveStartKey=start_key,
                Limit=limit,
            )
        items.extend(response.get("Items"))
        if "LastEvaluatedKey" in response:
            self.get_new_items(items, response["LastEvaluatedKey"])
        return items

    def get_items(
        self,
        filter_object_prefix: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 99,
    ) -> list[dict[str, any]]:
        """
        Get all items from the DynamoDB table
        :return: list of items

                "object_key",
                "stage",
                "status"
        """
        if filter_object_prefix and status:
            return self.dynamodb.scan(
                TableName=self.table,
                FilterExpression="begins_with(object_key, :prefix) AND #status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":prefix": {"S": filter_object_prefix},
                    ":status": {"S": status},
                },
                Limit=limit,
            ).get("Items", [])

        elif filter_object_prefix:
            return self.dynamodb.scan(
                TableName=self.table,
                FilterExpression="begins_with(object_key, :prefix)",
                ExpressionAttributeValues={":prefix": {"S": filter_object_prefix}},
                Limit=limit,
            ).get("Items", [])

        elif status:
            return self.dynamodb.scan(
                TableName=self.table,
                FilterExpression="#status = :status",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": {"S": status}},
                Limit=limit,
            ).get("Items", [])

        else:
            return self.dynamodb.scan(TableName=self.table, Limit=limit).get(
                "Items", []
            )

    def update_glue_job_item_status(
        self,
        key: str,
        status: str,
        pre_status: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> bool:
        """
        Update item status in the DynamoDB table
        :param key: item key
        :param status: item status
        :param pre_status: previous status
        :param timestamp: timestamp
        """
        if not timestamp:
            timestamp = datetime.now().strftime(ISO8901_FORMAT)
        try:
            if pre_status:
                self.dynamodb.update_item(
                    TableName=self.table,
                    Key={"object_key": {"S": key}},
                    UpdateExpression="SET #status = :status, #timestamp = :timestamp",
                    ExpressionAttributeNames={
                        "#status": "status",
                        "#timestamp": "timestamp",
                    },
                    ExpressionAttributeValues={
                        ":status": {"S": status},
                        ":pre_status": {"S": pre_status},
                        ":timestamp": {"S": timestamp},
                    },
                    ConditionExpression="attribute_exists(object_key) AND #status = :pre_status",
                )
            else:
                self.dynamodb.update_item(
                    TableName=self.table,
                    Key={"object_key": {"S": key}},
                    UpdateExpression="SET #status = :status, #timestamp = :timestamp",
                    ExpressionAttributeNames={
                        "#status": "status",
                        "#timestamp": "timestamp",
                    },
                    ExpressionAttributeValues={
                        ":status": {"S": status},
                        ":timestamp": {"S": timestamp},
                    },
                    ConditionExpression="attribute_exists(object_key)",
                )
            return True
        except ClientError as e:
            logger.error(f"Failed to update item {key} status. {str(e)}")
            return False

    def put_item(self, item: dict[str, any]) -> bool:
        """
        Put item to the DynamoDB table
        :param item: item to put
        """
        try:
            self.dynamodb.put_item(TableName=self.table, Item=item)
            return True
        except ClientError as e:
            raise e

    def query_ingest_task(
        self, object_key: str, jurisdiction: str, status: str
    ) -> dict[str, any]:
        """
        Query ingest task from the DynamoDB table
        :param object_key: object key
        :param jurisdiction: jurisdiction
        :param status: status
        :return: ingest task
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.table,
                Key={
                    "object_key": {"S": object_key},
                    "jurisdiction": {"S": jurisdiction},
                    "status": {"S": status},
                },
            )
            return response.get("Item")
        except ClientError as e:
            raise e

    def update_ingest_job_status(
        self, workload: IngestWorkload, from_status: str, to_status: str
    ) -> None:
        """
        Update the job status in the dynamodb table
        param: workload: IngestWorkload
        param: from_status: str
        param: to_status: str
        """
        try:
            self.dynamodb.update_item(
                TableName=self.table,
                Key={
                    "object_key": {"S": workload.object_key},
                    "jurisdiction": {"S": workload.jurisdiction},
                },
                UpdateExpression="SET #status = :to_status",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#timestamp": "timestamp",
                },
                ExpressionAttributeValues={
                    ":to_status": {"S": to_status},
                    ":from_status": {"S": from_status},
                    ":timestamp": {"N": str(workload.timestamp)},
                },
                ConditionExpression="#status = :from_status and #timestamp = :timestamp",
            )
        except ClientError as e:
            if "ConditionalCheckFailedException" in str(e):
                logger.warning(
                    f"Job {workload.object_key} status is not in the right state for update. Giving up."
                )
                raise e
            else:
                logger.error(
                    f"Failed to update item {workload.object_key} status. {str(e)}"
                )
                raise e
        return


def add_glue_job_task(s3_bucket: str, obj_key: str, file_size: str, target_table: str):
    """
    Add glue job task to the job queue
    :param s3_bucket: s3 bucket
    :param obj_key: object key
    :param file_size: file size
    :param target_table: target table
    :param region: aws region
    """
    job_item = Workload(
        bucket=s3_bucket,
        key=obj_key,
        size=str(file_size),
        status=JobStatus.NEW.name,
        timestamp=get_timestamp_string(),
        target=target_table,
    )
    aws_region = os.getenv("AWS_REGION", os.getenv("region", DEFAULT_REGION))
    dynamodb_client = boto3.client("dynamodb", region_name=aws_region)
    job_register_table = DynamoTable(
        table_name=os.getenv("JOB_REGISTER_TABLE", "glue-job-queue"),
        dynamodb=dynamodb_client,
    )
    job_register_table.put_item(item=job_item.model_dump())

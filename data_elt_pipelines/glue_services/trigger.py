import boto3
from botocore.exceptions import ClientError
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from glue_services.params import TriggerParams

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GlueTrigger:
    def __init__(self, glue_client: boto3.client):
        self.glue_client = glue_client

    @classmethod
    def get_trigger(cls, 
                    glue_client: boto3.client,
                    name: str) -> dict:
        """
        Retrieves metadata for a specified trigger.
        :param name: The name of the trigger to retrieve.
        :return: Metadata for the specified trigger.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-trigger.html#aws-glue-api-jobs-trigger-Trigger
        """
        try:
            response = glue_client.get_trigger(Name=name)
            trigger = response.get("Trigger")
            if trigger:
                logger.info(f"Trigger {trigger.get('Name')} found.")
            return trigger
        except ClientError as err:
            if err.response["Error"]["Code"] == "EntityNotFoundException":
                logger.info("Trigger %s not found.", name)
            else:
                logger.info(
                    "Couldn't get trigger %s. Here's why: %s: %s",
                    name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            return None
        
    async def create_trigger(self, trigger_params: TriggerParams) -> None:
        """
        Creates a trigger that starts an AWS Glue workflow.
        :param trigger_params: The parameters for the trigger.
        """
        try:
            trigger_meta = GlueTrigger.get_trigger(self.glue_client, trigger_params.Name)
            if trigger_meta:
                return trigger_meta.get("Name")

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                create_trigger_partial = partial(self.glue_client.create_trigger,
                                                 **trigger_params.model_dump(exclude_none=True)
                )
                                                    
                response = await loop.run_in_executor(pool, create_trigger_partial)
                return response.get("Name")
        except ClientError as err:
            logger.error(
                "Couldn't create trigger %s. Here's why: %s: %s",
                trigger_params.Name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"]
            )
            raise


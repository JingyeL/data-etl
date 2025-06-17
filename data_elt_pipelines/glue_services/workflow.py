import boto3
from botocore.exceptions import ClientError
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from glue_services.params import WorkflowParams

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GlueWorkflow:

    def __init__(self, glue_client: boto3.client):
        self.glue_client = glue_client

    @classmethod
    def get_workflow(cls,
                     glue_client: boto3.client,
                     name: str) -> dict:
        """
        Retrieves metadata for a specified workflow.
        :param name: The name of the workflow to retrieve.
        :return: Metadata for the specified workflow.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-workflow.html#aws-glue-api-workflow-Workflow
        """
        try:
            response = glue_client.get_workflow(Name=name)
            workflow = response.get("Workflow")
            if workflow:
                logger.info(f"Workflow {workflow.get('Name')} found.")
            return workflow
        except ClientError as err:
            if err.response["Error"]["Code"] == "EntityNotFoundException":
                logger.info("Workflow %s not found.", name)
            else:
                logger.info(
                    "Couldn't get workflow %s. Here's why: %s: %s",
                    name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            return None
        
    async def create_workflow(self, workflow_params: WorkflowParams) -> None:
        """
        Creates a workflow that represents a series of Glue tasks.
        :param workflow_params: The parameters for the workflow.
        """
        try:
            workflow_meta = GlueWorkflow.get_workflow(self.glue_client, workflow_params.Name)
            if workflow_meta:
                return workflow_meta.get("Name")
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                create_workflow_partial = partial(self.glue_client.create_workflow,
                                                    **workflow_params.model_dump(exclude_none=True))
                response = await loop.run_in_executor(
                    pool,
                    create_workflow_partial
                )
                return response.get("Name")

        except ClientError as err:
            logger.info(
                "Couldn't create workflow %s. Here's why: %s: %s",
                workflow_params.Name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

import boto3
from botocore.exceptions import ClientError
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from glue_services.params import JobParams


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GlueJob:
    glue_job_sys_args = {
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable",
        "--enable-continuous-log-filter": "true",
        "--continuous-log-filter": "INFO",
        "--enable-job-insights": "true",
        "--enable-observability-metrics": "true",
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--enable-continuous-cloudwatch-log": "false",
        "--datalake-formats": "iceberg",
        "--enable-auto-scaling": "true",
    }

    def __init__(self, glue_client):
        self.glue_client = glue_client

    @classmethod
    def get_job(cls, glue_client: boto3.client, name: str) -> str:
        """
        Retrieves metadata for a specified job.
        :param name: The name of the job to retrieve.
        :return: Metadata for the specified job.
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html
        """
        try:
            response = glue_client.get_job(JobName=name)
            job = response.get("Job")

            if job:
                logger.info(f"Job {job.get('Name')} found.")
            return job.get("Name")
        except ClientError as err:
            if err.response["Error"]["Code"] == "EntityNotFoundException":
                logger.info("Job %s not found.", name)
            else:
                logger.info(
                    "Couldn't get job %s. Here's why: %s: %s",
                    name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            raise

    async def create_job(self, job_params: JobParams) -> str:
        """
        Creates a job definition for an extract, transform, and load (ETL) job that can
        be run by AWS Glue.
        see: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-job.html#aws-glue-api-jobs-job-CreateJob

        :param name: The name of the job definition.
        :param description: The description of the job definition.
        :param role_arn: The ARN of an IAM role that grants AWS Glue the permissions
                         it requires to run the job.
        :param script_location: The Amazon S3 URL of a Python ETL script that is run as
                                part of the job. The script defines how the data is
                                transformed.
        """
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                create_job_partial = partial(
                    self.glue_client.create_job,
                    **job_params.model_dump(exclude_none=True),
                )
                response = await loop.run_in_executor(pool, create_job_partial)
            return response.get("Name")
        except ClientError as err:
            logger.error(
                "Couldn't create job %s. Here's why: %s: %s",
                job_params.Name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    async def update_job(self, job_name: str, job_params: JobParams) -> str:
        """
        Updates an existing job definition.
       """
    
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                update_job_partial = partial(
                    self.glue_client.update_job,
                    JobName=job_name,
                    JobUpdate=job_params.model_dump(exclude_none=True, exclude={"Name", "Tags"}),
                )
                response = await loop.run_in_executor(pool, update_job_partial)
            return response.get("Name")
        except ClientError as err:
            logger.error(
                "Couldn't update job %s. Here's why: %s: %s",
                job_params.Name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

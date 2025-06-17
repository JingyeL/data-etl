# write skeleton code for get or create a glue crawler

import boto3
from botocore.exceptions import ClientError
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from glue_services.params import CrawlerParams

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GlueCrawler:

    def __init__(self, glue_client: boto3.client):
        self.glue_client = glue_client

    @classmethod
    def get_crawler(cls,
                    glue_client: boto3.client,
                    name: str) -> dict:
        """
        Retrieves metadata for a specified crawler.
        :param name: The name of the crawler to retrieve.
        :return: Metadata for the specified crawler. 
        See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-crawling.html#aws-glue-api-crawler-crawling-Crawler
        """
        try:
            response = glue_client.get_crawler(Name=name)
            crawler = response.get("Crawler")

            if crawler:
                logger.info(f"Crawler {crawler.get("Name")} found.")
            return crawler
        except ClientError as err:
            if err.response["Error"]["Code"] == "EntityNotFoundException":
                logger.info("Crawler %s not found.", name)
            else:
                logger.info(
                    "Couldn't get crawler %s. Here's why: %s: %s",
                    name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            return None

    async def create_crawler(self, crawler_params: CrawlerParams) -> str:
        """
        Creates a crawler that can crawl the specified target and populate a
        database in your AWS Glue Data Catalog with metadata that describes the data
        in the target.
        :param crawler_params: The parameters for the crawler.
        :return: The name of the created crawler.
        """
        try:
            crawler_meta = GlueCrawler.get_crawler(self.glue_client, crawler_params.Name)
            if crawler_meta:
                return crawler_meta.get("Name")
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                create_crawler_partial = partial(
                    self.glue_client.create_crawler,
                    **crawler_params.model_dump(exclude_none=True)
                )
                response = await loop.run_in_executor(pool, create_crawler_partial)
                return response.get("Name")
        except ClientError as err:
            logger.error(
                "Couldn't create crawler %s. Here's why: %s: %s",
                crawler_params.Name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"]
            )
            raise
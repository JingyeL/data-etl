import asyncio
import boto3
import logging
import glue_services.params as glue_params
from glue_services.workflow import GlueWorkflow
from glue_services.trigger import GlueTrigger
from glue_services.job import GlueJob
from glue_services.crawler import GlueCrawler
from shared.tags import default_tags


logger = logging.getLogger(__name__)
glue_client = boto3.client("glue")


class GlueServiceWrapper:
    def __init__(
        self,
        glue_client: boto3.client,
        glue_service_role_arn: str,
        jurisdiction: str,
        source_parsing_script_location: str,
        cdm_mapping_script_location: str,
        glue_catalog_db: str,
        s3_data_warehouse_bucket: str,
    ):
        self.glue_client = glue_client
        self.role_arn = glue_service_role_arn
        self.jurisdiction = jurisdiction
        self.source_parsing_script_location = source_parsing_script_location
        self.cdm_mapping_script_location = cdm_mapping_script_location
        self.catalog_db = glue_catalog_db
        self.s3_data_warehouse_bucket = s3_data_warehouse_bucket

    async def create_glue_job(
        self, name: str, script_location: str, **job_args
    ) -> dict:
        """
        Create a Glue job for the jurisdiction
        :return: job metadata
        """
        description = get_standard_description("job", self.jurisdiction)
        default_job_args = {}
        for key, value in job_args.items():
            default_job_args[f"--{key}"] = value
        try:
            job_params = glue_params.JobParams(
                Name=name,
                Description=description,
                Role=self.role_arn,
                Command=glue_params.JobCommand(ScriptLocation=script_location),
                NonOverridableArguments=GlueJob.glue_job_sys_args,
                DefaultArguments=default_job_args,
                Tags=default_tags,
            )
        except Exception as e:
            logger.error(f"Error creating job params: {e}")
            raise
        job = GlueJob(self.glue_client)
        return await job.create_job(job_params)

    async def create_crawler(self, name: str, table_prefix: str) -> dict:
        """
        Create a Glue crawler for the jurisdiction
        :return: crawler metadata
        """
        description = get_standard_description("crawler", self.jurisdiction)
        crawling_targets = {}
        crawling_path = [
            f"s3://{self.s3_data_warehouse_bucket}/{self.jurisdiction}/source"
        ]
        crawling_targets = {"IcebergTargets": [{"Paths": crawling_path}]}

        crawler_params = glue_params.CrawlerParams(
            Name=name,
            Description=description,
            Role=self.role_arn,
            DatabaseName=self.catalog_db,
            TablePrefix=table_prefix,
            Targets=crawling_targets,
            Tags=self.default_tags,
        )
        crawler = GlueCrawler(self.glue_client)
        return await crawler.create_crawler(crawler_params)

    async def create_trigger(
        self,
        name: str,
        workflow_name: str,
        trigger_actions: list[glue_params.TriggerAction],
        trigger_type: glue_params.TriggerType,
        predicate: glue_params.TriggerPredicate = None,
    ) -> dict:
        """
        Create a Glue trigger for the raw data parser
        :param workflow_name: workflow name
        :return: trigger metadata
        """
        description = get_standard_description("trigger", self.jurisdiction)
        trigger_params = glue_params.TriggerParams(
            Name=name,
            Type=trigger_type,
            WorkflowName=workflow_name,
            Actions=trigger_actions,
            Description=description,
            Tags=self.default_tags,
            Predicate=predicate,
        )
        trigger = GlueTrigger(self.glue_client)
        return await trigger.create_trigger(trigger_params)

    async def create_glue_workflow(self, name: str) -> str:
        """
        Create a Glue workflow for the jurisdiction
        :param glue_client: glue client
        :param name: workflow name
        :return: workflow name
        """
        workflow_params = glue_params.WorkflowParams(
            Name=name,
            Description=get_standard_description("workflow", self.jurisdiction),
        )
        workflow = GlueWorkflow(self.glue_client)
        return await workflow.create_workflow(workflow_params)


def get_standard_description(entity: str, jurisdiction: str) -> str:
    """
    Get a standard description for the entity
    :param entity: entity
    :param jurisdiction: jurisdiction
    :return: description
    """
    return f"Glue {entity} for {jurisdiction}, created via SDK api, by the data engineering team"


def get_glue_workflow(glue_client: boto3.client, jurisdiction: str) -> dict:
    """
    Get the Glue workflow for the jurisdiction
    :param glue_client: glue client
    :param jurisdiction: jurisdiction
    :return: workflow metadata
    """
    response = GlueWorkflow.get_workflow(glue_client, name=jurisdiction)
    logger.debug(f"Workflow response: {response}")
    if not response:
        return None
    return response["Workflow"]


async def create_glue_etl_pipeline(
    glue_client: boto3.client,
    jurisdiction: str,
    source_parsing_script_location: str,
    cdm_mapping_script_location: str,
    glue_service_role_arn: str,
    catalog_db: str,
    s3_data_warehouse_bucket: str,
) -> dict:
    """
    Create a Glue ETL pipeline for the jurisdiction
    :param glue_client: glue client
    :param jurisdiction: jurisdiction
    :param source_parsing_script_location: source parsing script location
    :param cdm_mapping_script_location: cdm mapping script location
    :param glue_service_role_arn: glue service role ARN
    :param catalog_db: Glue catalog database
    :param s3_data_warehouse_bucket: S3 data warehouse bucket
    :return: ETL pipeline metadata
    """
    glue_service_wrapper = GlueServiceWrapper(
        glue_client=glue_client,
        glue_service_role_arn=glue_service_role_arn,
        jurisdiction=jurisdiction,
        source_parsing_script_location=source_parsing_script_location,
        cdm_mapping_script_location=cdm_mapping_script_location,
        glue_catalog_db=catalog_db,
        s3_data_warehouse_bucket=s3_data_warehouse_bucket,
    )
    raw_data_parser_job = f"raw-parser-{jurisdiction}"
    source_data_transformation_job = f"cdm-transformation-{jurisdiction}"
    source_data_crawler = f"source-crawler-{jurisdiction}"
    cdm_data_crawler = f"cdm-crawler-{jurisdiction}"
    workflow = f"workflow-{jurisdiction}"
    raw_trigger = f"trigger-raw-{jurisdiction}"
    source_trigger = f"trigger-source-{jurisdiction}"
    cdm_trigger = f"trigger-cdm-{jurisdiction}"

    def create_trigger_action(
        job_name: str, crawler_name: str, arguments: dict = None
    ) -> list[dict[str, str]]:
        if job_name:
            if arguments:
                return glue_params.TriggerAction(JobName=job_name, Arguments=arguments)
            else:
                return glue_params.TriggerAction(JobName=job_name, Arguments=arguments)
        if crawler_name:
            if arguments:
                return glue_params.TriggerAction(
                    CrawlerName=crawler_name, Arguments=arguments
                )
            else:
                return glue_params.TriggerAction(
                    CrawlerName=crawler_name, Arguments=arguments
                )

    raw_data_ready_trigger_actions = [create_trigger_action(raw_data_parser_job, None)]

    source_data_ready_trigger_actions = [
        create_trigger_action(source_data_transformation_job, None),
        create_trigger_action(None, source_data_crawler),
    ]

    cdm_data_ready_trigger_actions = [create_trigger_action(None, cdm_data_crawler)]

    source_data_trigger_predicate = glue_params.TriggerPredicate(
        Logical="AND",
        Conditions=[
            glue_params.TriggerCondition(
                JobName=raw_data_parser_job, State=glue_params.JobStateType.SUCCEEDED
            )
        ],
    )

    cdm_data_trigger_predicate = glue_params.TriggerPredicate(
        Logical="AND",
        Conditions=[
            glue_params.TriggerCondition(
                JobName=source_data_transformation_job,
                State=glue_params.JobStateType.SUCCEEDED,
            )
        ],
    )

    result = await asyncio.gather(
        glue_service_wrapper.create_glue_job(
            raw_data_parser_job, source_parsing_script_location
        ),
        glue_service_wrapper.create_glue_job(
            source_data_transformation_job, cdm_mapping_script_location
        ),
        glue_service_wrapper.create_crawler(source_data_crawler, "src_"),
        glue_service_wrapper.create_crawler(cdm_data_crawler, "stg_"),
        glue_service_wrapper.create_glue_workflow(workflow),
        glue_service_wrapper.create_trigger(
            name=raw_trigger,
            workflow_name=workflow,
            trigger_actions=raw_data_ready_trigger_actions,
            trigger_type=glue_params.TriggerType.ON_DEMAND,
        ),
        glue_service_wrapper.create_trigger(
            name=source_trigger,
            workflow_name=workflow,
            trigger_actions=source_data_ready_trigger_actions,
            trigger_type=glue_params.TriggerType.CONDITIONAL,
            predicate=source_data_trigger_predicate,
        ),
        glue_service_wrapper.create_trigger(
            name=cdm_trigger,
            workflow_name=workflow,
            trigger_actions=cdm_data_ready_trigger_actions,
            trigger_type=glue_params.TriggerType.CONDITIONAL,
            predicate=cdm_data_trigger_predicate,
        ),
    )
    if result:
        return {
            "raw_data_parser_job": result[0],
            "source_data_transformation_job": result[1],
            "source_data_crawler": result[2],
            "cdm_data_crawler": result[3],
            "workflow": result[4],
            "raw_trigger": result[5],
            "source_trigger": result[6],
            "cdm_trigger": result[7],
        }

# a generic lamdba handler for running ECS tasks in a container according to the input event
import os
import json
import logging
from shared.ecs_service import run_ecs_task
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """ """
    aws_region = os.getenv("AWS_REGION", os.getenv("region", event.get("region")))
    if not aws_region:
        logger.error("AWS region is required")
        raise ValueError("AWS region is required")

    ecs_cluster = event.get("ecs_cluster", os.getenv("ECS_CLUSTER"))
    if not ecs_cluster:
        logger.error("ECS cluster is required")
        raise ValueError("ECS cluster is required")

    ecs_task_definition_name = event.get("ecs_task_definition_name", os.getenv("ECS_TASK_DEF"))
    if not ecs_task_definition_name:
        logger.error("ECS task definition name is required")
        raise ValueError("ECS task definition name is required")

    ecs_container = event.get("ecs_container", os.getenv("ECS_CONTAINER"))
    if not ecs_container:
        logger.error("ecs_container is required")
        raise ValueError("ecs_container is required")

    instance_type = event.get("instance_type", os.getenv("INSTANCE_TYPE", "FARGATE"))

    subnet_ids = os.getenv("SUBNET_IDS", event.get("subnet_ids"))
    if not subnet_ids:
        logger.error("Subnet IDs are required")
        raise ValueError("Subnet IDs are required")
    subnets = subnet_ids.split(",")

    sg_groups = os.getenv("SG_GROUPS", event.get("sg_groups"))
    if not sg_groups:
        logger.error("Security Group IDs are required")
        raise ValueError("Security Group IDs are required")
    security_groups = sg_groups.split(",")

    if not event.get("payload"):
        payload = {}
    else:
        payload = json.loads(event.get("payload"))

    module = event.get("module")
    if not module:
        logger.error("Python Module name is required")
        raise ValueError("Python Module name is required")
    entryscript = event.get("entryscript", "ecs_wrapper.py")

    function_name = event.get("function_name", "lambda_handler")

    try:
        response = run_ecs_task(
            ecs_cluster=ecs_cluster,
            ecs_task_definition_name=ecs_task_definition_name,
            ecs_container=ecs_container,
            instance_type=instance_type,
            subnet_ids=subnets,
            sg_groups=security_groups,
            payload=payload,
            module_name=module,
            entryscript=entryscript,
            function_name=function_name,
        )
        logger.info(f"Response: {response}")
        if response.get("failures"):
            message = f"Failed to start ecs task. Failures: {response.get('failures')}"
            logger.error(message)
            return {"statusCode": 400, "body": message}
        else:
            logger.info("ECS task started successfully")
            return {
                "statusCode": 200,
                "body": json.dumps("ECS task started successfully"),
            }
    except Exception as e:
        logger.error(f"Failed to start ecs task. Error: {str(e)}")
        return {"statusCode": 500, "body": str(e)}
    except ClientError as e:
        logger.error(f"Failed to start ecs task  error: {str(e)}")
        return {"statusCode": 500, "body": str(e)}

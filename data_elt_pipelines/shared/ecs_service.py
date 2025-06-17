import os
import json
import boto3
import logging
from shared.constants import DEFAULT_REGION

logger = logging.getLogger()
ec2_client = boto3.client("ec2")


def run_ecs_task(
    ecs_cluster: str,
    ecs_task_definition_name: str,
    ecs_container: str,
    instance_type: str,
    subnet_ids: list[str],
    sg_groups: list[str],
    payload: dict[str, any],
    module_name: str,
    entryscript: str = "ecs_wrapper.py",
    function_name: str = "lambda_handler",
    **kwargs,
) -> dict[str, any]:
    """
    Run an ECS task with the given parameters
    :param ecs_cluster: ECS cluster name
    :param ecs_task_definition_name: ECS task definition name
    :param ecs_container: Container name
    :param instance_type: Instance type
    :param subnet_ids: List of subnet IDs
    :param sg_groups: List of security group IDs
    :param payload: Payload to pass to the container
    :param module_name: Python module name
    :param entryscript: Entry script
    :param function_name: Function name
    :return: ECS task response
    """
    python_run_time = kwargs.get("PYTHON_RUN_TIME", "python3.12")
    ecs_client = boto3.client("ecs")
    container_override = {
        "name": ecs_container,
        "command": [
            f"{python_run_time} {entryscript} --function {function_name} --event '{json.dumps(payload)}' --module {module_name}"
        ]
    }
    if kwargs.get("memory"):
        container_override["memory"]= int(kwargs.get("memory"))

    return ecs_client.run_task(
        cluster=ecs_cluster,
        taskDefinition=ecs_task_definition_name,
        launchType=instance_type,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnet_ids,
                "securityGroups": sg_groups,
                "assignPublicIp": "ENABLED",
            }
        },
        overrides={
            "containerOverrides": [container_override],
        },
    )

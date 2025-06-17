import os
import json
import boto3
import pytest
from unittest.mock import patch, MagicMock
from shared.ecs_service import run_ecs_task


@pytest.fixture
def ecs_client_mock():
    with patch("boto3.client") as mock:
        yield mock


def test_run_ecs_task(ecs_client_mock):
    ecs_cluster = "test-cluster"
    ecs_task_definition_name = "test-task"
    ecs_container = "test-container"
    instance_type = "FARGATE"
    subnet_ids = ["subnet-12345"]
    sg_groups = ["sg-12345"]
    payload = {"key": "value"}
    module_name = "test_module"
    entryscript = "ecs_wrapper.py"
    function_name = "lambda_handler"
    python_run_time = "python3.12"
    memory = 512

    os.environ["PYTHON_RUN_TIME"] = python_run_time

    ecs_client_instance = MagicMock()
    ecs_client_mock.return_value = ecs_client_instance

    expected_command = [
        f"{python_run_time} {entryscript} --function {function_name} --event '{json.dumps(payload)}' --module {module_name}"
    ]

    response = run_ecs_task(
        ecs_cluster,
        ecs_task_definition_name,
        ecs_container,
        instance_type,
        subnet_ids,
        sg_groups,
        payload,
        module_name,
        entryscript,
        function_name,
        memory=memory,
    )

    ecs_client_mock.assert_called_once_with("ecs")
    ecs_client_instance.run_task.assert_called_once_with(
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
            "containerOverrides": [
                {"name": ecs_container, 
                 "command": expected_command,
            "memory": 512}]
        },
    )
    assert response == ecs_client_instance.run_task.return_value

def test_run_ecs_task_not_python_runtime_no_memory(ecs_client_mock):
    ecs_cluster = "test-cluster"
    ecs_task_definition_name = "test-task"
    ecs_container = "test-container"
    instance_type = "FARGATE"
    subnet_ids = ["subnet-12345"]
    sg_groups = ["sg-12345"]
    payload = {"key": "value"}
    module_name = "test_module"
    entryscript = "ecs_wrapper.py"
    function_name = "lambda_handler"

    ecs_client_instance = MagicMock()
    ecs_client_mock.return_value = ecs_client_instance

    expected_command = [
        f"python3.12 {entryscript} --function {function_name} --event '{json.dumps(payload)}' --module {module_name}"
    ]

    # Act
    response = run_ecs_task(
        ecs_cluster,
        ecs_task_definition_name,
        ecs_container,
        instance_type,
        subnet_ids,
        sg_groups,
        payload,
        module_name,
        entryscript,
        function_name,
    )

    # Assert
    ecs_client_mock.assert_called_once_with("ecs")
    ecs_client_instance.run_task.assert_called_once_with(
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
            "containerOverrides": [
                {"name": ecs_container, "command": expected_command}]
        },
    )
    assert response == ecs_client_instance.run_task.return_value



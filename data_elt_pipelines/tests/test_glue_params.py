import pytest
from pydantic import ValidationError
from glue_services.params import (
    TriggerType,
    JobStateType,
    TriggerCondition,
    TriggerPredicate,
    TriggerAction,
    TriggerParams,
    CrawlerParams,
    WorkflowParams,
    JobCommand,
    JobParams,
)


# TriggerParams
def test_trigger_params():
    action = TriggerAction(JobName="test_job", Timeout=5)
    condition = TriggerCondition(JobName="test_job", State=JobStateType.RUNNING)
    predicate = TriggerPredicate(Logical="AND", Conditions=[condition])
    params = TriggerParams(
        Name="test_trigger",
        Type=TriggerType.SCHEDULED,
        Actions=[action],
        Predicate=predicate,
    )
    assert params.Name == "test_trigger"
    assert params.Type == TriggerType.SCHEDULED
    assert params.Actions[0].JobName == "test_job"
    assert params.Predicate.Conditions[0].JobName == "test_job"


def test_trigger_condition_serialization():
    condition = TriggerCondition(JobName="test_job", State=JobStateType.RUNNING)
    serialized = condition.model_dump()
    assert serialized["State"] == "RUNNING"


def test_trigger_predicate_serialization():
    condition = TriggerCondition(JobName="test_job", State=JobStateType.RUNNING)
    predicate = TriggerPredicate(Logical="AND", Conditions=[condition])
    serialized = predicate.model_dump()
    assert serialized["Conditions"][0]["State"] == "RUNNING"


def test_trigger_action_validation():
    with pytest.raises(ValidationError):
        TriggerAction(Timeout=-1)

    action = TriggerAction(JobName="test_job", Timeout=5)
    assert action.Timeout == 5


# CrawlerParams
def test_crawler_params():
    targets = {"S3Targets": [{"Path": "s3://bucket/path"}]}
    params = CrawlerParams(
        Name="test_crawler",
        Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
        DatabaseName="test_db",
        TablePrefix="test_",
        Targets=targets,
    )
    assert params.Name == "test_crawler"
    assert params.Targets["S3Targets"][0]["Path"] == "s3://bucket/path"
    assert (
        params.Role == "arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole"
    )
    assert params.DatabaseName == "test_db"
    assert params.TablePrefix == "test_"


def test_crawler_params_missing_required_fields():
    targets = {"S3Targets": [{"Path": "s3://bucket/path"}]}
    with pytest.raises(ValidationError):
        CrawlerParams(
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            DatabaseName="test_db",
            TablePrefix="test_",
            Targets=targets,
        )

    with pytest.raises(ValidationError):
        CrawlerParams(
            Name="test_crawler",
            DatabaseName="test_db",
            TablePrefix="test_",
            Targets=targets,
        )

    with pytest.raises(ValidationError):
        CrawlerParams(
            Name="test_crawler",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            TablePrefix="test_",
            Targets=targets,
        )

    with pytest.raises(ValidationError):
        CrawlerParams(
            Name="test_crawler",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            DatabaseName="test_db",
            Targets=targets,
        )

    with pytest.raises(ValidationError):
        CrawlerParams(
            Name="test_crawler",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            DatabaseName="test_db",
            TablePrefix="test_",
        )


def test_crawler_params_invalid_targets():
    with pytest.raises(ValidationError):
        CrawlerParams(
            Name="test_crawler",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            DatabaseName="test_db",
            TablePrefix="test_",
            Targets={"S3Targets":"path"},
        )


# WorkflowParams
def test_workflow_params():
    params = WorkflowParams(Name="test_workflow", Description="A test workflow")
    assert params.Name == "test_workflow"
    assert params.Description == "A test workflow"


def test_workflow_params_missing_required_fields():
    with pytest.raises(ValidationError):
        WorkflowParams(Description="A test workflow")


def test_workflow_params_minimal():
    params = WorkflowParams(Name="test_workflow")
    assert params.Name == "test_workflow"
    assert not params.Description


def test_workflow_params_invalid_name():
    with pytest.raises(ValidationError):
        WorkflowParams(Name=123)


def test_workflow_params_invalid_default_run_properties():
    with pytest.raises(ValidationError):
        WorkflowParams(Name="test_workflow", DefaultRunProperties="invalid_type")


# JobParams
def test_job_params_default_values():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    with pytest.raises(ValidationError):
        JobParams(
            Name="test_job",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            Command=command,
            Timeout=-1,
        )

    params = JobParams(
        Name="test_job",
        Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
        Command=command
    )
    assert params.Timeout == 10
    assert params.GlueVersion == "4.0"
    assert params.WorkerType == "G.1X"
    assert params.NumberOfWorkers == 2

def test_job_params_valid():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    params = JobParams(
        Name="test_job",
        Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
        Command=command,
        Timeout=5,
        GlueVersion="2.0",
        WorkerType="G.2X",
        NumberOfWorkers=5,
        NonOverridableArguments={"key": "value"},
        Tags={"key2": "value2"},
    )
    assert params.Name == "test_job"
    assert (
        params.Role == "arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole"
    )
    assert params.Command.ScriptLocation == "s3://scripts/test_script.py"
    assert params.Timeout == 5
    assert params.GlueVersion == "2.0"
    assert params.WorkerType == "G.2X"
    assert params.NumberOfWorkers == 5
    assert params.NonOverridableArguments == {"key": "value"}
    assert params.Tags == {"key2": "value2"}


def test_job_params_glue_version_validation():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    with pytest.raises(ValidationError):
        JobParams(
            Name="test_job",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            Command=command,
            GlueVersion="invalid_version",
        )


def test_job_params_worker_type_validation():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    with pytest.raises(ValidationError):
        JobParams(
            Name="test_job",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            Command=command,
            WorkerType="invalid_worker_type",
        )


def test_job_params_missing_required_fields():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    with pytest.raises(ValidationError):
        JobParams(
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            Command=command,
        )

    with pytest.raises(ValidationError):
        JobParams(Name="test_job", Command=command)

    with pytest.raises(ValidationError):
        JobParams(
            Name="test_job",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
        )


def test_job_params_invalid_timeout():
    command = JobCommand(ScriptLocation="s3://scripts/test_script.py")
    with pytest.raises(ValidationError):
        JobParams(
            Name="test_job",
            Role="arn:aws:iam::123456789012:role/service-role/AWSGlueServiceRole",
            Command=command,
            Timeout=-1,
        )
from pydantic import BaseModel, field_validator, field_serializer
from typing import Optional, List, Dict, Any
from enum import Enum


class TriggerType(Enum):
    SCHEDULED = "SCHEDULED"
    CONDITIONAL = "CONDITIONAL"
    ON_DEMAND = "ON_DEMAND"
    EVENT = "EVENT"


class JobStateType(Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    WAITING = "WAITING"
    EXPIRED = "EXPIRED"


class CrawlStateType(Enum):
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ERROR = "ERROR"


class TriggerCondition(BaseModel):
    """
    Model representing a trigger condition configuration.

    Attributes:
        JobName (str): The name of the Glue job.
        CrawlerName (str): The name of the Glue crawler.
        State (str): The state of the job.
        CrawlState (str): The state of the crawler.
    """

    LogicalOperator: Optional[str] = "EQUALS"
    JobName: Optional[str] = None
    CrawlerName: Optional[str] = None
    State: Optional[JobStateType] = JobStateType.SUCCEEDED
    CrawlState: Optional[CrawlStateType] = CrawlStateType.SUCCEEDED

    @field_serializer("State")
    @classmethod
    def serialize_state(cls, val: JobStateType) -> str:
        return val.name

    @field_serializer("CrawlState")
    @classmethod
    def serialize_crawl_state(cls, val: CrawlStateType) -> str:
        return val.name


class TriggerPredicate(BaseModel):
    """
    Model representing a trigger predicate configuration.

    Attributes:
        Logical (str): The logical operator for the predicate.
        Conditions (list[TriggerCondition]): The conditions for the predicate.
    """

    Logical: str
    Conditions: list[TriggerCondition]

    @field_serializer("Conditions")
    @classmethod
    def serialize_conditions(
        cls, conditions: List[TriggerCondition]
    ) -> List[Dict[str, Any]]:
        return [condition.model_dump(exclude_none=True) for condition in conditions]


class TriggerAction(BaseModel):
    """
    Model representing a Glue trigger action configuration.
    Attributes:
        JobName (Optional[str]): The name of the Glue job to trigger, one of JobName or CrawlerName is required.
        CrawlerName (Optional[str]): The name of the Glue crawler to trigger.
        Arguments (dict[str, str]): The arguments to pass to the Glue job or crawler.
        Timeout (int): The JobRun timeout in minutes. This is the maximum time that a job run can consume resources before it is terminated and enters TIMEOUT status.
                       The default is 2,880 minutes (48 hours). This overrides the timeout value set in the parent job.
        SecurityConfiguration (Optional[str]): The name of the security configuration to use.
        NotifyDelayAfter (Optional[int]): The delay in minutes before sending a notification after the trigger action starts.
        NotificationProperty (dict): A dictionary containing notification properties, specifically "NotifyDelayAfter".
    """

    JobName: Optional[str] = None
    CrawlerName: Optional[str] = None
    Arguments: Optional[Dict[str, str]] = None
    Timeout: Optional[int] = 10

    @field_validator("Timeout")
    def check_positive_timeout(cls, value):
        if value is not None and value <= 0:
            raise ValueError("Timeout must be a positive integer")
        return value

    SecurityConfiguration: Optional[str] = None
    NotificationProperty: Optional[Dict[str, Any]] = {"NotifyDelayAfter": 3}

    @field_validator("NotificationProperty")
    @classmethod
    def check_notify_delay_after(cls, value):
        if value is not None and "NotifyDelayAfter" in value:
            if value["NotifyDelayAfter"] <= 0:
                raise ValueError("  must be a positive integer")
        return value


class TriggerParams(BaseModel):
    """
    Model representing a trigger configuration.

    Attributes:
        Name (str): The name of the trigger.
        WorkflowName (str): The name of the workflow associated with the trigger.
        Type (TriggerType): The type of the trigger.
        Actions (list[TriggerAction]): The actions to perform when the trigger fires.
        Description (str): A description of the trigger.
        Schedule (str): The schedule for the trigger.
        Predicate (TriggerPredicate): The predicate for the trigger.
        Tags (dict[str, str]): The tags for the trigger.
        StartOnCreation (bool): A boolean indicating whether to start the trigger when it is created.
    """

    Name: str
    WorkflowName: Optional[str] = None
    Type: TriggerType
    Actions: List[TriggerAction]
    Description: Optional[str] = None
    Schedule: Optional[str] = None
    Predicate: Optional[TriggerPredicate] = None
    Tags: Optional[Dict[str, str]] = None
    StartOnCreation: Optional[bool] = False

    @field_serializer("Type")
    @classmethod
    def serialize_type(cls, val: TriggerType) -> str:
        return val.name

    @field_serializer("Predicate")
    @classmethod
    def serialize_predicate(cls, val: TriggerPredicate) -> Dict[str, Any]:
        return val.model_dump(exclude_none=True)

    @field_serializer("Actions")
    @classmethod
    def serialize_actions(cls, actions: List[TriggerAction]) -> List[Dict[str, Any]]:
        return [action.model_dump(exclude_none=True) for action in actions]


class CrawlerParams(BaseModel):
    """
    Model representing a Crawler configuration.

    Attributes:
        Name (str): The name of the crawler.
        Role (str): The ARN of the role used by the crawler.
        DatabaseName (str): The name of the database to populate with the crawler.
        TablePrefix (str): The prefix to add to the tables created by the crawler.
        SchemaChangePolicy (dict[str, str]): The schema change policy for the crawler.
        Targets (dict[str, list[dict[str, Any]]): The targets for the crawler. e.g. {"S3Targets": [{"Path": "s3://bucket/path"}]}
        Tags (dict[str, str]): The tags for the
    """

    Name: str
    Role: str
    Description: Optional[str] = None
    DatabaseName: str
    TablePrefix: str
    SchemaChangePolicy: Optional[Dict[str, str]] = {
        "UpdateBehavior": "UPDATE_IN_DATABASE",
        "DeleteBehavior": "DELETE_FROM_DATABASE",
    }
    Targets: Dict[str, List[Dict[str, Any]]]
    Tags: Optional[Dict[str, str]] = None


class WorkflowParams(BaseModel):
    """
    Model representing a workflow configuration.

    Attributes:
        Name (str): The name of the workflow.
        Description (str): A description of the workflow.
        DefaultRunProperties (dict[str, str]): The default run properties for the workflow.
    """

    Name: str
    Description: Optional[str] = None
    DefaultRunProperties: Optional[dict[str, str]] = None
    # DefaultRunProperties=workflow_params.default_run_properties)


class JobCommand(BaseModel):
    """
    Model representing a job command configuration.
    """
    Name: str = "glueetl"
    ScriptLocation: str
    PythonVersion: str = "3"


class JobParams(BaseModel):
    """
    Model representing a job configuration.

    Attributes:
        Name (str): The name of the job.
        Description (str): A description of the job.
        Role (str): The ARN of the role used by the job.
        Command (JobCommand): The command to run for the job.
        NoOverridableArguments (dict[str, str]): The non-overridable arguments for the job.
        DefaultArguments (dict[str, str]): The default arguments for the job.
        Connections (dict[str, str]): The connections for the job.
        MaxRetries (int): The maximum number of retries for the job.
        Timeout (int): The timeout for the job.
        GlueVersion (str): The Glue version for the job.
        WorkerType (str): The worker type for the job.
        NumberOfWorkers (int): The number of workers for the job.
    """

    Name: str
    Description: Optional[str] = None
    Role: str
    Command: JobCommand
    NonOverridableArguments: Optional[Dict[str, str]] = None
    DefaultArguments: Optional[Dict[str, str]] = None
    Connections: Optional[Dict[str, str]] = None
    MaxRetries: Optional[int] = 0
    Timeout: Optional[int] = 10
    GlueVersion: Optional[str] = "4.0"
    WorkerType: Optional[str] = "G.1X"
    #  To use Worker Type G.1X, minimum allowed value of Number of Workers is 2.
    # A single standard DPU provides 4 vCPU and 16 GB of memory
    NumberOfWorkers: Optional[int] = 2
    Tags: Optional[Dict[str, str]] = None
    ExecutionProperty: Optional[Dict[str, Any]] = {"MaxConcurrentRuns": 3}

    @field_validator("Timeout")
    def check_positive_timeout(cls, value):
        if value <= 0:
            raise ValueError("timeout must be a positive integer")
        return value

    @field_validator("GlueVersion")
    def check_glue_version(cls, value):
        valid_versions = {"2.0", "3.0", "4.0"}
        if value not in valid_versions:
            raise ValueError(f"glue_version must be one of {valid_versions}")
        return value

    @field_validator("WorkerType")
    def check_worker_type(cls, value):
        valid_worker_types = {"G.1X", "G.2X", "G.4X", "G.8X"}
        if value not in valid_worker_types:
            raise ValueError(f"worker_type must be one of {valid_worker_types}")
        return value
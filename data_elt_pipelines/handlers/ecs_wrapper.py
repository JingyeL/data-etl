# A wrapper script that enables Lambda functions to run as ECS Fargate tasks. This allows for processing of large datasets that exceed Lambda's memory and execution time limits.

# **Purpose:**
# - Executes any Lambda handler function within an ECS container environment
# - Provides scalable processing for large files and long-running operations
# - Maintains consistent logging output to CloudWatch via stdout streaming
# - Dynamically imports and executes specified modules and functions

# **Features:**
# - **Dynamic Module Loading**: Imports any handler module at runtime
# - **Structured Logging**: Configures CloudWatch-compatible logging with timestamps
# - **Error Handling**: Comprehensive error reporting with context information
# - **Flexible Execution**: Supports any Lambda function signature

# **Usage:**
# The wrapper is called from within ECS containers with command-line arguments:

# ```bash
# python ecs_wrapper.py --event '{"key": "value"}' --module "handler_module" --function "lambda_handler"
# ```

# **Arguments:**
# - `--event`: JSON string containing the event payload for the Lambda function
# - `--module`: Python module name containing the handler function
# - `--function`: Function name to execute (typically "lambda_handler")

# **Example ECS Task Invocations:**

# ```json
# // Run SFTP download as ECS task
# {
#   "ecs_cluster": "jingyel-data-etl-cluster",
#   "ecs_task_definition_name": "jingyel-ecs-data-etl",
#   "ecs_container": "data_etl",
#   "module": "fetcher_ftp_download_handler",
#   "payload": "{\"jurisdiction\": \"us_fl\", \"periodicity\": \"quarterly\"}"
# }

# // Run file unzipping as ECS task
# {
#   "ecs_cluster": "jingyel-data-etl-cluster", 
#   "ecs_task_definition_name": "jingyel-ecs-data-etl",
#   "ecs_container": "data_etl",
#   "module": "archive_utility_handler", 
#   "payload": "{\"action\": \"unzip\", \"bucket\": \"jingyel-data-etl-raw\", \"object_keys\": \"us_fl/data.zip\"}"
# }
# ```

# **Container Configuration:**
# - Uses stdout streaming for CloudWatch log integration
# - Maintains current working directory context
# - Provides detailed error information including module paths and directory listings
# - Compatible with Fargate task definitions

# **Error Reporting:**
# When errors occur, the wrapper logs:
# - Exception details and stack trace
# - Module and function names being executed
# - Event payload content
# - Current working directory
# - Directory listing for debugging

# **Integration:**
# The wrapper is used by the `ecs_task_handler` Lambda function to create Fargate tasks that can process large datasets without Lambda's resource constraints.

import os
import argparse
import json
import logging
import sys
import importlib

logger = logging.getLogger()

# for container to explictly stream logs to stdout so that it can be captured by cloudwatch logs
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def main():
    parser = argparse.ArgumentParser(description="Fetch data from FTP and upload to S3")
    parser.add_argument("--event", type=str, help="payload")
    parser.add_argument("--module", type=str, help="module name")
    parser.add_argument("--function", type=str, help="function name")
    args = parser.parse_args()
    
    event = json.loads(args.event)
    try:
        module = importlib.import_module(args.module)
        func = getattr(module, args.function)
        func(event, None)
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(f"Module: {args.module}")
        logger.error(f"Function: {args.function}")
        logger.error(f"Event: {json.dumps(event)}")
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"context: {os.listdir()}")
        raise e


if __name__ == "__main__":
    main()

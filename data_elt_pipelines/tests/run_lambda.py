#####################################
# Test the lambda functions locally
#####################################
import argparse
import handlers
import handlers.data_bulk_load_handler
import handlers.fetcher_ftp_download_handler
import handlers.fixed_width_text_handler
import handlers.schema_transformation_handler
import handlers.glue_services_handler
import handlers.ingestion_services_handler
import handlers.archive_utility_handler
import handlers.ecs_task_handler


def argument_parser():
    """
    Argument parser
    :return: arguments
    """
    parser = argparse.ArgumentParser(description="Test the lambda functions")
    parser.add_argument(
        "--operation",
        "-o",
        type=str,
        required=True,
        help="Operation",
        # list of operations
        choices=[
            "fetch",
            "fetch_workload",
            "parse-fwtxt",
            "parse-html",
            "map",
            "load",
            "glue",
            "unzip",
            "ecs-task",
        ],
    )
    parser.add_argument(
        "--bucket",
        "-b",
        type=str,
        required=False,
        default="oc-poc-data-raw",
        help="Bucket",
    )
    parser.add_argument(
        "--object_key", "-k", type=str, required=False, help="Object Key", default="xyz"
    )
    parser.add_argument(
        "--jurisdiction",
        "-j",
        type=str,
        required=False,
        help="Jurisdiction",
        default="us_fl",
    )
    parser.add_argument(
        "--action",
        "-a",
        type=str,
        required=False,
        help="reload",
        # list of operations
        choices=["reload", "ADD_WORKLOAD", "START", "RESET", "RETRY"],
    )
    parser.add_argument(
        "--stage", "-s", type=str, required=False, help="staging|source"
    )
    parser.add_argument(
        "--status",
        "-u",
        type=str,
        required=False,
        help="NEW|PROCESSING|PROCESSED|ERROR",
    )

    return parser.parse_args()


if __name__ == "__main__":
    arg = argument_parser()
    if arg.operation == "fetch_workload":
        event = {
            "action": "START",
        }
        handlers.ingestion_services_handler.lambda_handler(event, context=None)
    if arg.operation == "fetch":
        event = {
            "jurisdiction": "us_fl",
            "object_key": "doc/Quarterly/Cor/cordata.zip",
            "periodicity": "quarterly",
            "timestamp": "1728403080",
            "status": "PREPARING",
            "ingest_config": "us_fl_historical",
            "region": "eu-west-2",
            "DYNAMO_TABLE": "etl-job-queue",
            "RAW_DATA_BUCKET": "data-pipeline-play-poc-data-raw",
            "CONFIG_BUCKET": "data-pipeline-play-poc-data-config",
        }

        # event = {
        #     "jurisdiction": "us_fl",
        #     "object_key": "doc/cor/20241119c.txt",
        #     "periodicity": "daily",
        #     "timestamp": "0",
        #     "status": "NEW",
        #     "ingest_config": "us_fl",
        #     "region": "eu-west-2",
        #     "DYNAMO_TABLE": "etl-job-queue",
        #     "RAW_DATA_BUCKET": "data-pipeline-play-poc-data-raw",
        #     "CONFIG_BUCKET": "data-pipeline-play-poc-data-config",
        # }
        handlers.fetcher_ftp_download_handler.lambda_handler(event, context=None)
    else:
        event = {"bucket": arg.bucket, "key": arg.object_key}
        if arg.operation == "parse-fwtxt":
            event["action"] = arg.action if arg.action else None
            event["jurisdiction"] = arg.jurisdiction
            event["key"] = arg.object_key
            event["raw_data_bucket"] = arg.bucket
            handlers.fixed_width_text_handler.lambda_handler(event, context=None)
        elif arg.operation == "parse-html":
            pass
        elif arg.operation == "map":
            event["key"] = arg.object_key
            event["bucket"] = arg.bucket

            handlers.schema_transformation_handler.lambda_handler(event, context=None)
        elif arg.operation == "load":
            handlers.data_bulk_load_handler.lambda_handler(event, context=None)
        elif arg.operation == "glue":
            event["stage"] = arg.stage
            event["action"] = arg.action
            event["status"] = arg.status
            handlers.glue_services_handler.lambda_handler(event, context=None)
        elif arg.operation == "unzip":
            event["action"] = "unzip"
            event["object_keys"] = arg.object_key
            event["bucket"] = arg.bucket
            handlers.archive_utility_handler.lambda_handler(event, context=None)
        elif arg.operation == "ecs-task":
            event = {
                "ecs_cluster": "data-pipeline-play-data-etl-cluster",
                "ecs_task_definition_name": "data-pipeline-play-ecs_data_etl",
                "ecs_container": "data_etl",
                "module": "archive_utility_handler",
                "payload": '{"action": "unzip", "bucket": "data-pipeline-play-poc-data-raw", "object_keys": "us_fl_historical/2024/10/08/cordata.zip"}',
            }
            handlers.ecs_task_handler.lambda_handler(event, context=None)

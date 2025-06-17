FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.12 as lambda_base_image
ENV region=${region}

FROM lambda_base_image as python_base
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt
COPY shared ${LAMBDA_TASK_ROOT}/shared

FROM python_base as ingestion_services
ENV RAW_DATA_BUCKET=${RAW_DATA_BUCKET}
ENV CONFIG_BUCKET=${CONFIG_BUCKET}
COPY handlers/ingestion_services_handler.py ${LAMBDA_TASK_ROOT}
COPY source_ingestion ${LAMBDA_TASK_ROOT}/source_ingestion
COPY requirements_ingest.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements_ingest.txt
CMD ["ingestion_services_handler.lambda_handler"]

FROM ingestion_services as fetcher_ftp_download
ENV RAW_DATA_BUCKET=${RAW_DATA_BUCKET}
ENV CONFIG_BUCKET=${CONFIG_BUCKET}
COPY handlers/fetcher_ftp_download_handler.py ${LAMBDA_TASK_ROOT}
CMD ["fetcher_ftp_download_handler.lambda_handler"]

FROM python_base as archive_utility
COPY handlers/archive_utility_handler.py ${LAMBDA_TASK_ROOT}
CMD ["archive_utility_handler.lambda_handler"]

FROM python_base as fixed_width_txt
ENV LAMBDA_HANDLER=fixed_width_text_handler
ENV LAMBDA_NAME=fixed_width_text_parser
ENV SOURCE_DATA_BUCKET=${SOURCE_DATA_BUCKET}
ENV CONFIG_BUCKET=${CONFIG_BUCKET}
COPY handlers/fixed_width_text_handler.py ${LAMBDA_TASK_ROOT}
COPY raw_parsers ${LAMBDA_TASK_ROOT}/raw_parsers
CMD ["fixed_width_text_handler.lambda_handler"]

FROM python_base as cdm_mapper
ENV LAMBDA_HANDLER=schema_transformation_handler
ENV LAMBDA_NAME=schema_transformation
ENV CDM_DATA_BUCKET=${CDM_DATA_BUCKET}
ENV CONFIG_BUCKET=${CONFIG_BUCKET}
COPY handlers/schema_transformation_handler.py ${LAMBDA_TASK_ROOT}
COPY ${LAMBDA_NAME} ${LAMBDA_TASK_ROOT}/${LAMBDA_NAME}
CMD ["schema_transformation_handler.lambda_handler"]

FROM python_base as bulk_loader
ENV db_conn_name=${db_conn_name}
COPY requirements_db.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements_db.txt
COPY handlers/data_bulk_load_handler.py ${LAMBDA_TASK_ROOT}
CMD ["data_bulk_load_handler.lambda_handler"]

FROM python_base as data_etl_pipeline
COPY handlers/glue_services_handler.py ${LAMBDA_TASK_ROOT}
COPY glue_services ${LAMBDA_TASK_ROOT}/glue_services
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt
CMD ["glue_services_handler.lambda_handler"]

# FROM --platform=arm64 public.ecr.aws/amazonlinux/amazonlinux:2023 as ecs_data_etl
FROM python_base as ecs_data_etl
RUN dnf update -y && \
    dnf install -y amazon-cloudwatch-agent collectd
ENV region=${region}
ENV TASK_ROOT=/app
RUN mkdir -p ${TASK_ROOT}
COPY cloudwatch-agent-config.json /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
COPY handlers/*.py ${TASK_ROOT}/.
COPY raw_parsers ${TASK_ROOT}/raw_parsers
COPY shared ${TASK_ROOT}/shared
COPY source_ingestion ${TASK_ROOT}/source_ingestion
COPY schema_transformation ${TASK_ROOT}/schema_transformation
COPY requirements*.txt ${TASK_ROOT}
WORKDIR ${TASK_ROOT}
RUN pip install -r requirements_ingest.txt
RUN pip install -r requirements_db.txt

ENTRYPOINT ["/bin/sh", "-c"]
# , "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent -config /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json && exec \"$@\""]
CMD ["python3 ecs_wrapper.py --event '{}' --module fetcher_ftp_download_handler --function lambda_handler"]

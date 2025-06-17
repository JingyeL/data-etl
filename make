# !/bin/bash
# Check if account_id is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <account_id> <env>"
  exit 1
fi

account_id=$1
env=$2

echo "Using account: $account_id ($env)"
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin $account_id.dkr.ecr.eu-west-2.amazonaws.com
poetry run pytest
if [ $? -ne 0 ]; then
    printf "\e[31mTests failed. Please fix before proceeding.\e[0m"
    exit 1
fi

rm requirements*.txt
poetry export -f requirements.txt --output requirements.txt --without-hashes
poetry export --without-hashes -f requirements.txt --output requirements_db.txt  --with db
poetry export --without-hashes -f requirements.txt --output requirements_ingest.txt  --with ingest

export sftp_fetcher_download_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-fetcher_ftp_download:latest
docker buildx build --target fetcher_ftp_download --platform linux/arm64 --tag fetcher_ftp_download:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag fetcher_ftp_download:latest $sftp_fetcher_download_image_uri
docker push $sftp_fetcher_download_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-fetcher_ftp_download \
    --image-uri $sftp_fetcher_download_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null

export archive_utility_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-archive_utility:latest
docker buildx build --target archive_utility --platform linux/arm64 --tag archive_utility:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag archive_utility:latest $archive_utility_image_uri
docker push $archive_utility_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-archive_utility \
    --image-uri $archive_utility_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null


export ingestion_services_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-ingestion_services:latest
docker buildx build --target ingestion_services --platform linux/arm64 --tag ingestion_services:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag ingestion_services:latest $ingestion_services_image_uri
docker push $ingestion_services_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-ingestion_services \
    --image-uri $ingestion_services_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null

export data_etl_pipeline_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-glue_service:latest
docker buildx build --target data_etl_pipeline --platform linux/arm64 --tag glue_service:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag glue_service:latest $data_etl_pipeline_image_uri
docker push $data_etl_pipeline_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-glue_service \
    --image-uri $data_etl_pipeline_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null

export bulk_loader_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-data_bulk_load:latest
docker buildx build --target bulk_loader --platform linux/arm64 --tag bulk_loader:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag bulk_loader:latest $bulk_loader_image_uri
docker push $bulk_loader_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-data_bulk_load \
    --image-uri $bulk_loader_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null

export fixed_width_parser_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-fixed_width_txt:latest
docker buildx build --target fixed_width_txt --platform linux/arm64 --tag fixed_width_txt:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag fixed_width_txt:latest $fixed_width_parser_image_uri
docker push $fixed_width_parser_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-fixed_width_txt \
    --image-uri $fixed_width_parser_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null

export cdm_mapper_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-schema_transformation:latest
docker buildx build --target cdm_mapper --platform linux/arm64 --tag cdm-mapper:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag cdm-mapper:latest $cdm_mapper_image_uri
docker push $cdm_mapper_image_uri
aws lambda update-function-code --function-name data-pipeline-$env-schema_transformation \
    --image-uri $cdm_mapper_image_uri \
    --architectures arm64 \
    --region eu-west-2 >/dev/null


export ecs_data_etl_image_uri=$account_id.dkr.ecr.eu-west-2.amazonaws.com/data-pipeline-$env-ecs_data_etl:latest
docker buildx build --target ecs_data_etl --platform linux/arm64 --tag ecs_data_etl:latest .
if [ $? -ne 0 ]; then
    printf "\e[31mBuild failed. Please fix before proceeding.\e[0m"
    exit 1
fi
docker tag ecs_data_etl:latest $ecs_data_etl_image_uri
docker push $ecs_data_etl_image_uri


# backfill_file_meta
export temp_path=../infra/module/python/backfill_file_meta
rm -rf $temp_path
mkdir -p $temp_path/build/shared
mkdir -p $temp_path/dist
cp handlers/backfill_file_meta_handler.py $temp_path/build/.
cp -r shared/*.py $temp_path/build/shared/.

# update_metadata_timestamp
export temp_path=../infra/module/python/update_metadata_timestamp
rm -rf $temp_path
mkdir -p $temp_path/build/shared
mkdir -p $temp_path/dist
cp handlers/update_metadata_timestamp_handler.py $temp_path/build/.
cp -r shared/*.py $temp_path/build/shared/.

# # glue job scripts
export temp_path=../infra/module/lake/dist/glue_job_scripts/
rm -rf $temp_path
mkdir -p $temp_path/
cp glue_jobs/*.py $temp_path/.

cp -r shared $temp_path/.
cp -r schema_transformation $temp_path/.
cd $temp_path
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type d -name .pytest_cache -exec rm -rf {} +
zip -r glue_job.zip .
cd -

# # ecs task handler
export temp_path=../infra/module/python/ecs_task/
rm -rf $temp_path
mkdir -p $temp_path/build/shared
mkdir -p $temp_path/dist
cp -r shared/*.py $temp_path/build/shared/.
cp handlers/ .py $temp_path/build/.

rm requirements*.txt
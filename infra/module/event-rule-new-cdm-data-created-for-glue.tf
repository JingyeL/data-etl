resource "aws_cloudwatch_event_rule" "new_cdm_data_created_for_glue" {
  name        = "data-pipeline-new-cdm-data-created-for-glue-job"
  description = "Fires when a new source data file is created in ${aws_s3_bucket.cdm_data.bucket} bucket."

  event_pattern = <<PATTERN
{
  "version": ["0"],
  "detail-type": ["Object Created"],
  "source": ["aws.s3"],
  "account": ["${local.account_id}"],
  "region": ["${local.aws_region}"],
  "resources": [
    "${aws_s3_bucket.cdm_data.arn}"
  ],
  "detail": {
    "version": ["0"],
    "bucket": {
      "name": ["${aws_s3_bucket.cdm_data.bucket}"]
    },
    "object": {
      "key": [
        {
          "suffix": ".csv.bz2"
        }
      ]
    }
  }
}
PATTERN
}

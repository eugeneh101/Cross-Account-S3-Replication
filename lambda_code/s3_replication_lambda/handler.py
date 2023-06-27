import json
import os
import tempfile

import boto3


DESTINATION_BUCKET_NAME = os.environ["DESTINATION_BUCKET_NAME"]
DESTINATION_BUCKET_PREFIX = os.environ["DESTINATION_BUCKET_PREFIX"]
CREDENTIALS_SECRET_NAME = os.environ["CREDENTIALS_SECRET_NAME"]

sm_client = boto3.client("secretsmanager")
CREDENTIALS = json.loads(
    sm_client.get_secret_value(SecretId=CREDENTIALS_SECRET_NAME)["SecretString"]
)
s3_resource = boto3.resource("s3")


def lambda_handler(event, context) -> None:
    # only want batch size of 1 so easier to deal with failing SQS messages
    assert len(event["Records"]) == 1, f"There should be only 1 record in: {event}"
    nested_event = json.loads(event["Records"][0]["body"])
    assert (
        len(nested_event["Records"]) == 1
    ), f"There should be only 1 record in: {nested_event}"
    s3_file_details = nested_event["Records"][0]["s3"]
    s3_bucket = s3_file_details["bucket"]["name"]
    s3_key = s3_file_details["object"]["key"]
    assert s3_key.startswith(
        DESTINATION_BUCKET_PREFIX
    ), f'S3 file key should be in folder "{DESTINATION_BUCKET_PREFIX}" but it is "{s3_key}"'

    print(f"Attempting to download s3://{s3_bucket}/{s3_key}")
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_filename_on_disk = os.path.join(tmpdir, os.path.basename(s3_key))
        s3_resource.Bucket(name=s3_bucket).download_file(
            Key=s3_key,
            Filename=temp_filename_on_disk,
        )
        s3_resource_external = boto3.resource(
            "s3",
            aws_access_key_id=CREDENTIALS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=CREDENTIALS["AWS_SECRET_ACCESS_KEY"],
        )
        print(f"Attempting to upload to s3://{DESTINATION_BUCKET_NAME}/{s3_key}")
        s3_resource_external.Bucket(name=DESTINATION_BUCKET_NAME).upload_file(
            Filename=temp_filename_on_disk, Key=s3_key
        )
        print(f"Successfully uploaded to s3://{DESTINATION_BUCKET_NAME}/{s3_key}")

import aws_cdk as cdk
import boto3

from cross_account_s3_replication import CrossAccountS3ReplicationStack


app = cdk.App()
environment = app.node.try_get_context("environment")
account = boto3.client("sts").get_caller_identity()["Account"]
CrossAccountS3ReplicationStack(
    app,
    "CrossAccountS3ReplicationStack",
    env=cdk.Environment(account=account, region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()

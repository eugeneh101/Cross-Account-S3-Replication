from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class CrossAccountS3ReplicationStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.s3_cross_account_replication_role = iam.Role(
            self,
            "S3CrossAccountReplicationRole",
            assumed_by=iam.ServicePrincipal("s3.amazonaws.com"),
            path="/service-role/",
            role_name=environment["S3_REPLICATION_ROLE_NAME"],
        )

        self.s3_bucket_source = s3.Bucket(
            self,
            "S3BucketSource",
            bucket_name=environment["SOURCE_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,  # if versioning disabled, then expired files are deleted
        )

        # connect AWS resources
        self.s3_cross_account_replication_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:Get*"],
                resources=[
                    self.s3_bucket_source.bucket_arn,
                    self.s3_bucket_source.arn_for_objects("*"),
                ],
            )
        )
        s3_bucket_destination = s3.Bucket.from_bucket_arn(
            self,
            "S3BucketDestination",
            bucket_arn=f"arn:aws:s3:::{environment['DESTINATION_BUCKET_NAME']}",
        )
        self.s3_cross_account_replication_role.add_to_policy(
            iam.PolicyStatement(
                resources=[s3_bucket_destination.arn_for_objects("*")],
                actions=[
                    "s3:ReplicateObject",
                    "s3:ReplicateDelete",
                    "s3:ReplicateTags",
                    "s3:GetObjectVersionTagging",
                ],
            )
        )
        if environment["DESTINATION_BUCKET_EXISTS"]:
            self.s3_bucket_source.node.default_child.replication_configuration = (
                s3.CfnBucket.ReplicationConfigurationProperty(
                    role=self.s3_cross_account_replication_role.role_arn,
                    rules=[
                        s3.CfnBucket.ReplicationRuleProperty(
                            destination=s3.CfnBucket.ReplicationDestinationProperty(
                                bucket=s3_bucket_destination.bucket_arn,
                                account=environment["DESTINATION_BUCKET_ACCOUNT"],
                            ),
                            status="Enabled",
                        )
                    ],
                )
            )

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_events as events,
    aws_events_targets as event_targets,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs as sqs,
)
from constructs import Construct


class CrossAccountS3ReplicationStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.s3_replication_role = iam.Role(
            self,
            "S3ReplicationRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=environment["S3_REPLICATION_ROLE_NAME"],
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.s3_replication_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    (
                        "arn:aws:secretsmanager:*:*:secret:"
                        f"{environment['CREDENTIALS_SECRET_NAME']}*"  # need asterisk as ARN has weird suffix
                    ),
                ],
            )
        )
        self.s3_replication_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:PutBucketNotification",
                    "s3:GetObject",
                ],
                resources=["*"],
            )
        )
        self.s3_replication_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueUrl",
                    "sqs:GetQueueAttributes",
                ],
                resources=["*"],
            )
        )
        # prevent any changes to the policies, equivalent to mutable=False
        self.s3_replication_role = self.s3_replication_role.without_policy_updates()

        self.s3_bucket_source = s3.Bucket(
            self,
            "S3BucketSource",
            bucket_name=environment["SOURCE_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            notifications_handler_role=self.s3_replication_role,
        )

        self.s3_notifications_dlq = sqs.Queue(
            self,
            "S3NotificationsDLQ",
            queue_name=environment["QUEUE_NAME"] + "_dlq",  # hard coded suffix
            visibility_timeout=Duration.seconds(30),  # hard coded
            retention_period=Duration.days(7),  # hard coded
        )
        self.s3_notifications_queue = sqs.Queue(
            self,
            "S3NotificationsQueue",
            queue_name=environment["QUEUE_NAME"],
            visibility_timeout=Duration.seconds(
                environment["LAMBDA_RUNTIME_IN_SECONDS"] + 1
            ),  # hard coded addition
            retention_period=Duration.days(7),  # hard coded
            dead_letter_queue={
                "queue": self.s3_notifications_dlq,
                "max_receive_count": 2,  # hard coded to allow 1 retry
            },
        )

        self.s3_replication_lambda = _lambda.Function(
            self,
            "S3ReplicationLambda",
            function_name=environment["LAMBDA_FUNCTION_NAME"],
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/s3_replication_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(
                environment["LAMBDA_RUNTIME_IN_SECONDS"]
            ),  # to replicate very large files
            memory_size=environment[
                "LAMBDA_MEMORY_IN_MB"
            ],  # to replicate very large files
            ephemeral_storage_size=Size.mebibytes(
                environment["LAMBDA_EPHEMERAL_STORAGE_IN_MB"]
            ),  # to replicate very large files
            environment={
                "DESTINATION_BUCKET_NAME": environment["DESTINATION_BUCKET_NAME"],
                "DESTINATION_BUCKET_PREFIX": environment["DESTINATION_BUCKET_PREFIX"],
                "CREDENTIALS_SECRET_NAME": environment["CREDENTIALS_SECRET_NAME"],
            },
            role=self.s3_replication_role,
        )
        self.alarm_lambda = _lambda.Function(
            self,
            "AlarmLambda",
            function_name="alarm-lambda",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.InlineCode("def lambda_handler(event, context): print(event)"),
            handler="index.lambda_handler",
            role=self.s3_replication_role,
        )

        # having 2 targets for fun: SNS is direct, Eventbridge is indirect
        self.alarm_topic = sns.Topic(
            self, "AlarmTopic", topic_name="alarm-topic"  # hard coded
        )
        self.alarm_action = cloudwatch_actions.SnsAction(topic=self.alarm_topic)
        self.event_rule_to_trigger_alarm_lambda = events.Rule(
            self,
            "EventRuleToTriggerAlarmLambda",
            rule_name="alarm-rule",  # hard coded
            event_bus=None,  # want "default" bus
        )

        # connect AWS resources together
        self.s3_bucket_source.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(self.s3_notifications_queue),
        )
        self.s3_replication_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.s3_notifications_queue,
                batch_size=1,  # hard coded to simplify if Lambda fails to process SQS message
            )
        )
        self.lambda_alarm = cloudwatch.Alarm(
            self,
            "LambdaAlarm",
            alarm_name=environment["LAMBDA_FUNCTION_NAME"] + "-alarm",  # hard coded
            alarm_description="If the replication Lambda fails, notify me",
            metric=self.s3_replication_lambda.metric_errors(
                statistic="sum", period=Duration.minutes(1)  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.lambda_alarm.add_alarm_action(self.alarm_action)
        self.lambda_alarm.add_ok_action(self.alarm_action)
        self.alarm_topic.add_subscription(
            sns_subs.LambdaSubscription(self.alarm_lambda)
        )
        self.event_rule_to_trigger_alarm_lambda.add_event_pattern(
            source=["aws.cloudwatch"],
            detail_type=["CloudWatch Alarm State Change"],
            resources=[self.lambda_alarm.alarm_arn],
        )
        self.event_rule_to_trigger_alarm_lambda.add_target(
            event_targets.LambdaFunction(
                self.alarm_lambda,  # dead_letter_queue=..., retry_attempts=2
            )
        )

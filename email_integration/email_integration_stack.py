from aws_cdk import (
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
    aws_glue as glue,
    aws_ses as ses,
    aws_ses_actions as actions,
    aws_iam as iam,
    aws_s3_deployment as s3deploy,
    aws_lambda as lambda_,
    aws_lakeformation as lakeformation,
    aws_sns_subscriptions as subscriptions,
    core
)


class EmailIntegrationStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        ingest_configuration = self.node.try_get_context('IngestEmailConfiguration')
        DATALAKE_ACCOUNT = core.Aws.ACCOUNT_ID
        GLUE_DATABASE_NAME = ingest_configuration.get('GLUE_DATABASE_NAME')
        S3_PREFIX_RAW = ingest_configuration.get('S3_PREFIX_RAW')
        S3_PREFIX_QUARANTINE = ingest_configuration.get('S3_PREFIX_QUARANTINE')
        S3_PREFIX_CURATED = ingest_configuration.get('S3_PREFIX_CURATED')
        SES_RECIPIENT = ingest_configuration.get('SES_RECIPIENT')
        CONFIG_PARSER_KEY = ingest_configuration.get('CONFIG_PARSER_KEY')
        OPS_TEAM_EMAIL = ingest_configuration.get('OPS_TEAM_EMAIL')

        email_integration_bucket = s3.Bucket(self, "s3-email-integration-stream",
                                             bucket_name=f"email-integration-{DATALAKE_ACCOUNT}",
                                             encryption=s3.BucketEncryption.S3_MANAGED)

        role_glue_lambda = iam.Role(
            self,
            id="glue_permissions_role",
            managed_policies=[
                iam.ManagedPolicy.from_managed_policy_arn(self, id="glue_permission_policy",
                                                          managed_policy_arn="arn:aws:iam::aws:policy/"
                                                                             "AWSGlueConsoleFullAccess"),
                iam.ManagedPolicy.from_managed_policy_arn(self, id="cloudwatch_permission_policy",
                                                          managed_policy_arn="arn:aws:iam::aws:policy/"
                                                                             "service-role/AWSLambdaBasicExecutionRole")
            ],
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        email_filtering_function = lambda_.DockerImageFunction(
            self,
            "email_filtering",
            function_name=f"email_filtering_{core.Aws.ACCOUNT_ID}",
            description="Email filter",
            code=lambda_.DockerImageCode.from_image_asset("./src/lambdas", cmd=["email_filtering.lambda_handler"]),
            timeout=core.Duration.seconds(30),
            memory_size=128,
            environment={
                "BUCKET_NAME": email_integration_bucket.bucket_name,
                "CONFIG_PARSER_KEY": CONFIG_PARSER_KEY
            }
        )

        email_processing_function = lambda_.DockerImageFunction(
            self,
            "email_processing",
            function_name=f"email_processing_{core.Aws.ACCOUNT_ID}",
            code=lambda_.DockerImageCode.from_image_asset("./src/lambdas", cmd=["email_processing.lambda_handler"]),
            description="Email processor",
            timeout=core.Duration.minutes(5),
            memory_size=512,
            role=role_glue_lambda,
            environment={
                "POSSIBLE_EXTENSION_FILE": "csv, xls, xlsx",
                "GLUE_DATABASE_NAME": GLUE_DATABASE_NAME,
                "S3_PREFIX_RAW": S3_PREFIX_RAW,
                "S3_PREFIX_CURATED": S3_PREFIX_CURATED,
                "S3_PREFIX_QUARANTINE": S3_PREFIX_QUARANTINE,
            }
        )

        s3deploy.BucketDeployment(self, "DeployConfigFile",
                                  sources=[s3deploy.Source.asset("./config")],
                                  destination_bucket=email_integration_bucket,
                                  destination_key_prefix="config/"
                                  )

        email_integration_bucket.grant_read_write(identity=email_filtering_function)
        email_integration_bucket.grant_read_write(identity=email_processing_function)

        queue_for_quarantine_objects = sqs.Queue(self, "Quarantine_Queue",
                                                 queue_name=f"Quarantine_Queue_{core.Aws.ACCOUNT_ID}")

        topic_for_quarantine_objects = sns.Topic(self, "Quarantine_Topic",
                                                 display_name="Malformed Email subscription topic"
                                                 )

        topic_for_quarantine_objects.add_subscription(
            subscription=subs.SqsSubscription(queue_for_quarantine_objects))
        topic_for_quarantine_objects.add_subscription(subscriptions.EmailSubscription(OPS_TEAM_EMAIL))

        email_integration_bucket.add_event_notification(s3.EventType.OBJECT_CREATED_PUT,
                                                        s3n.SnsDestination(topic_for_quarantine_objects),
                                                        s3.NotificationKeyFilter(
                                                            prefix=f"{S3_PREFIX_QUARANTINE}/"
                                                        )
                                                        )

        email_integration_bucket.add_event_notification(s3.EventType.OBJECT_CREATED_PUT,
                                                        s3n.LambdaDestination(email_processing_function),
                                                        s3.NotificationKeyFilter(
                                                            prefix=f"{S3_PREFIX_RAW}/"
                                                        )
                                                        )

        email_integration_db=glue.Database(self, "emailIntegrationSystemGlueDB",
                      database_name=GLUE_DATABASE_NAME
                      )

        lf_db_permissions=lakeformation.CfnPermissions(
            self,
            "LakeFormationLambdaRoleOnTheDB",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role_glue_lambda.role_arn),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(name=email_integration_db.database_name)
            ),
            permissions=["CREATE_TABLE"]

        )
        lf_table_permissions=lakeformation.CfnPermissions(
            self,
            "LakeFormationLambdaRoleOnTheDBTables",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role_glue_lambda.role_arn),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(name=email_integration_db.database_name),
                table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                    database_name=email_integration_db.database_name,
                    table_wildcard={},
                )
            ),
            permissions=["ALL"]

        )


        ses.ReceiptRuleSet(self, "RuleSetSinkToS3",
                           rules=[
                               ses.ReceiptRuleOptions(recipients=[SES_RECIPIENT],
                                                      actions=[actions.Lambda(
                                                          function=email_filtering_function,
                                                          invocation_type=actions.LambdaInvocationType.REQUEST_RESPONSE
                                                      ),
                                                          actions.S3(
                                                              bucket=email_integration_bucket,
                                                              object_key_prefix=S3_PREFIX_RAW,

                                                          )])]
                           )

from aws_cdk import core
from email_integration.email_integration_stack import EmailIntegrationStack
from aws_cdk.assertions import Template, Match

TESTING_CONTEXT = {
    "IngestEmailConfiguration": {
        "GLUE_DATABASE_NAME": "database_email_integration",
        "S3_PREFIX_RAW": "tooling",
        "S3_PREFIX_QUARANTINE": "quarantine_email",
        "S3_PREFIX_CURATED": "curated_emails",
        "SES_RECIPIENT": "email_you_own@server.com",
        "ACCEPTED_SENDERS": "trusted_emails@server.com,emailtest2@email.com",
        "CONFIG_PARSER_KEY": "config/email.json",
        "OPS_TEAM_EMAIL":"foo@bar.com"
    }
}


def get_template():
    app = core.App(
        context=TESTING_CONTEXT
    )
    return Template.from_stack(EmailIntegrationStack(app, "cdk-email-integration"))


my_cdk_template = get_template()


def test_count_glue_database_resource():
    my_cdk_template.resource_count_is("AWS::Glue::Database", 1)


def test_ses_rule_set():
    expected = {
        "Rule": {
            "Actions": Match.array_equals([
                Match.object_like({"LambdaAction": Match.any_value()}),
                Match.object_like({"S3Action": Match.any_value()})
            ]),

            "Enabled": True,
            "Recipients": Match.any_value()
        },
    }
    my_cdk_template.has_resource_properties("AWS::SES::ReceiptRule", expected)

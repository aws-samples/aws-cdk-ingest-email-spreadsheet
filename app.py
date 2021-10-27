#!/usr/bin/env python3

from aws_cdk import core

from email_integration.email_integration_stack import EmailIntegrationStack

app = core.App()
EmailIntegrationStack(app, "cdk-email-integration")

app.synth()

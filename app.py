#!/usr/bin/env python3

from aws_cdk import App

from email_integration.email_integration_stack import EmailIntegrationStack

app = App()
EmailIntegrationStack(app, "cdk-email-integration")

app.synth()

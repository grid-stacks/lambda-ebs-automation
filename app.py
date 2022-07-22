#!/usr/bin/env python3
import os

import aws_cdk as cdk
from dotenv import load_dotenv

from lambda_ebs_automation.lambda_ebs_automation_stack import LambdaEbsAutomationStack

load_dotenv()

AWS_ACCOUNT = os.getenv('AWS_ACCOUNT')
AWS_REGION = os.getenv('AWS_REGION')

app = cdk.App()
LambdaEbsAutomationStack(
    app, "LambdaEbsAutomationStack",
    env=cdk.Environment(account=AWS_ACCOUNT, region=AWS_REGION),
)
app.synth()

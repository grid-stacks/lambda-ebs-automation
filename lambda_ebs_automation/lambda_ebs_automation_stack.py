from aws_cdk import (Duration, Stack, aws_lambda as _lambda, aws_iam as _iam, )
from constructs import Construct


class LambdaEbsAutomationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        backup_ebs_role = _iam.Role(
            scope=self, id='BackupEbsRole',
            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
            role_name='BackupEbsRole',
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name('AmazonEC2FullAccess'),
                _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
            ]
        )

        backup_ebs_role.add_to_policy(_iam.PolicyStatement(
            actions=["logs:*"],
            resources=["arn:aws:logs:*:*:*"]
        ))
        backup_ebs_role.add_to_policy(_iam.PolicyStatement(
            actions=[
                "ec2:CreateSnapshot",
                "ec2:DeleteSnapshot",
                "ec2:CreateTags",
                "ec2:ModifySnapshotAttribute",
                "ec2:ResetSnapshotAttribute"
            ],
            resources=["*"]
        ))
        backup_ebs_role.add_to_policy(_iam.PolicyStatement(
            actions=["ec2:Describe*"],
            resources=["*"]
        ))

        backup_ebs_lambda = _lambda.Function(self, 'BackupEbsHandler', runtime=_lambda.Runtime.PYTHON_3_9,
                                             code=_lambda.Code.from_asset('lambda'), handler='backup_ebs.handler',
                                             timeout=Duration.minutes(15), role=backup_ebs_role)

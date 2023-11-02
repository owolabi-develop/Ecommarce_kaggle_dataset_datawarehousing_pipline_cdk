from constructs import Construct
import aws_cdk as cdk
from  aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    Duration,
    aws_iam as _iam,
)

Environment = {
    "GLUE_JOB_NAME":""
}

class TriggerFuncStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **Kwargs):
        super().__init__(scope, construct_id, **Kwargs)
        
        
        
        lambda_role = _iam.Role(
            self,
            "lambdaRole",
             assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
             managed_policies=[
                 _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                 _iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
                 _iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
                
             ]
        )
        
      
        
        
        
        ecommarce_trigger = _lambda.Function(self,
                                             "propertiesdataproducer",
                                             runtime=_lambda.Runtime.PYTHON_3_12,
                                             code=_lambda.Code.from_asset("lambda"),
                                             handler="lambda.handler",
                                             timeout=Duration.minutes(3),
                                             role=lambda_role,
                                             environment = Environment
                                           
                                             )
from constructs import Construct
import aws_cdk as cdk
from  aws_cdk import (
    Stack,
     aws_s3 as _s3,
    RemovalPolicy,
    aws_sqs as _sqs ,
     aws_s3_deployment as _s3deploy,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as _lambda_event_source,
    aws_lambda as _lambda,
    aws_iam as _iam,
    Duration,
)



class RawBuscketStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **Kwargs):
        super().__init__(scope, construct_id, **Kwargs)
    
            
        ecommarce_raw_zone_bucket = _s3.Bucket(self,
                                     "ecommarce_raw_zone_bucket",
                                     bucket_name='ecommarce-raw-zone',
                                     removal_policy=RemovalPolicy.DESTROY,
                                     auto_delete_objects=True
                              )
        
        consumption_zone_bucket = _s3.Bucket(self,
                                             "ConsumptionZonebucket",
                                            bucket_name='ecommarce-consumption-zone',
                                            removal_policy=RemovalPolicy.DESTROY,
                                            auto_delete_objects=True
                              )
        
        
        ### upload all data set to ecommarce_raw_zone_bucket
        _s3deploy.BucketDeployment(self,"upload",
                                  sources=[_s3deploy.Source.asset('dataset/')],
                                  destination_bucket=ecommarce_raw_zone_bucket)
        
        
        lambda_role = _iam.Role(
            self,
            "lambdaRole",
             assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
             managed_policies=[
                 _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                 _iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
                 _iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueServiceRole"),
                 _iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess") 
             ]
        )
        
      
        
        
        
        ecommarce_trigger = _lambda.Function(self,
                                             "propertiesdataproducer",
                                             runtime=_lambda.Runtime.PYTHON_3_12,
                                             code=_lambda.Code.from_asset("lambda"),
                                             handler="lambda_trigger.handler",
                                             timeout=Duration.minutes(3),
                                             role=lambda_role
                                        
                                             )
        
        ## sqs to   ecommarce_bucket_queue
        ecommarce_queue = _sqs.Queue(self, 'ecommarce_queue',
                                            visibility_timeout=cdk.Duration.seconds(300))
        
        ecommarce_raw_zone_bucket.add_object_created_notification(s3n.SqsDestination(ecommarce_queue))
        
        
        ## sqs to  tigger lambda function to trigger all glue job
        ecommarce_trigger.add_event_source(_lambda_event_source.SqsEventSource(ecommarce_queue))
        
        
        
        
        
     
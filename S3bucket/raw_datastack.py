from constructs import Construct
import aws_cdk as cdk
from  aws_cdk import (
    Stack,
     aws_s3 as _s3,
    RemovalPolicy,
    aws_sqs as _sqs ,
     aws_s3_deployment as _s3deploy,
    aws_s3_notifications as s3n,
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
        
        
        ### upload all data set to 
        _s3deploy.BucketDeployment(self,"deployment",
                                  sources=[_s3deploy.Source.asset('dataset/')],
                                  destination_bucket=ecommarce_raw_zone_bucket)
        
        
        
        ## sqs que to trigger lambda
        ecommarce_bucket_queue = _sqs.Queue(self, 'ecommarce_queue')
        ecommarce_raw_zone_bucket.add_event_notification(_s3.EventType.OBJECT_CREATED, s3n.SqsDestination(ecommarce_bucket_queue))
        
        
     
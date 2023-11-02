from constructs import Construct
import aws_cdk as cdk
from  aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    Duration,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_glue as _glue,
    aws_s3_deployment as _s3deploy,
    aws_lakeformation as _lakeformation,
    RemovalPolicy,
    aws_sqs as _sqs ,
    aws_events as _events,
    aws_s3_notifications as s3n,
)
import aws_cdk.aws_s3_deployment as s3deploy


Ecommarce_raw_zone_ARN = "arn:aws:s3bucket:us-east-1:521427190825:stream/ecommarce-raw-zone"

Ecommarce_raw_zone_ARN = "arn:aws:s3bucket:us-east-1:521427190825:stream/ecommarce-consumption-zone"


class GlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **Kwargs):
        super().__init__(scope, construct_id, **Kwargs)
        
        
        
        lambda_role = _iam.Role(
            self,
            "lambdaRole",
             assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
             managed_policies=[
                 _iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
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
                                           
                                             )
        
        ## glue script bucket
        glue_script_bucket = _s3.Bucket(self,
                                             "gluescriptbucket",
                                            bucket_name='ecommarce-glue-script-bucket',
                                            removal_policy=RemovalPolicy.DESTROY,
                                            auto_delete_objects=True
                              )
        
        ### deploy the glue script 
        
        s3deploy.BucketDeployment(self,"deployment",
                                  sources=[s3deploy.Source.asset('glue/glueScript')],
                                  destination_bucket=glue_script_bucket)
        
        
        
        glue_role = _iam.Role(self, 'glue_role',
                      role_name='GlueRole',
                      description='Role for Glue services to access S3',
                      assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
                      inline_policies={'glue_policy': _iam.PolicyDocument(
                          statements=[_iam.PolicyStatement(
                            effect=_iam.Effect.ALLOW,
                            actions=['s3:*', 'glue:*', 'iam:*', 'logs:*',
                            'cloudwatch:*', 'sqs:*', 'ec2:*','cloudtrail:*'],
                            resources=['*'])])})

        glue_database = _glue.CfnDatabase(self, 'ecommarcedatabase',
                                        catalog_id=cdk.Aws.ACCOUNT_ID,
                                        database_input=_glue.CfnDatabase.DatabaseInputProperty(
                                            name='ecommarce-database',
                                            description='Database to store ecommarce details'))

        _lakeformation.CfnPermissions(self, 'lakeformation_permission',
                    data_lake_principal=_lakeformation.CfnPermissions.DataLakePrincipalProperty(
                        data_lake_principal_identifier=glue_role.role_arn),
                    resource=_lakeformation.CfnPermissions.ResourceProperty(
                        database_resource=_lakeformation.CfnPermissions.DatabaseResourceProperty(
                            catalog_id=glue_database.catalog_id,
                            name='ecommarce-database')),
                    permissions=['ALL'])
        
        
        
        _glue.CfnCrawler(self, 'glue_crawler',
                 name='ecommarce_crawler',
                 role=glue_role.role_arn,
                 database_name='ecommarce-database',
                 targets=_glue.CfnCrawler.TargetsProperty(
                     s3_targets=[_glue.CfnCrawler.S3TargetProperty(
                         path=f's3://ecommarce-raw-zone/',
                         #event_queue_arn=glue_queue.queue_arn
                         )]),
                 recrawl_policy=_glue.CfnCrawler.RecrawlPolicyProperty(
                     recrawl_behavior='CRAWL_EVENT_MODE'))

        glue_job = _glue.CfnJob(self, 'glue_job',
                                name='glue_job',
                                command=_glue.CfnJob.JobCommandProperty(
                                    name='pythonshell',
                                    python_version='3.9',
                                    script_location=f's3://{glue_script_bucket.bucket_name}/glue_job.py'),
                                role=glue_role.role_arn,
                                glue_version='3.0',
                                timeout=3)
        
        
        
       
        
       
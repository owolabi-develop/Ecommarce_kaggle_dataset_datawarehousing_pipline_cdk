#!/usr/bin/env python3
import os

import aws_cdk as cdk


from glue.glue_stack import GlueStack
from S3bucket.raw_datastack import RawBucketStack

env_US = cdk.Environment(account="521427190825",region='us-east-1')

app = cdk.App()

GlueStack(app,"GlueStack",env=env_US)

RawBucketStack(app,"RawBucketStack",env=env_US)

cdk.Tags.of(app).add("ProjectOwner","Owolabi akintan")
cdk.Tags.of(app).add("ProjectName","ecommarce-dataset-warehousing-pipline")

app.synth()

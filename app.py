#!/usr/bin/env python3
import os

import aws_cdk as cdk

from glue.glue_stack import GlueStack
from S3bucket.raw_datastack import RawBuscketStack

env_US = cdk.Environment(account="521427190825",region='us-east-1')

app = cdk.App()
GlueStack(app,"GlueStack",env=env_US)
RawBuscketStack(app," RawBuscketStack",env=env_US)

app.synth()

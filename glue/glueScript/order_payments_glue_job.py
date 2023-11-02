import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


PaymentAWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database="ecommarce-database",
    table_name="olist_order_payments_dataset_csv",
    transformation_ctx="PaymentAWSGlueDataCatalog ",
)


PaymentChangeSchema = ApplyMapping.apply(
    frame=PaymentAWSGlueDataCatalog,
    mappings=[
        ("order_id", "string", "order_id", "string"),
        ("payment_sequential", "long", "payment_sequential", "long"),
        ("payment_type", "string", "payment_type", "string"),
        ("payment_installments", "long", "payment_installments", "long"),
        ("payment_value", "double", "payment_value", "double"),
    ],
    transformation_ctx="PaymentChangeSchema",
)


S3_consumption_bucket = glueContext.write_dynamic_frame.from_options(
    frame=PaymentChangeSchema,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://ecommarce-consumption-zone",
        "partitionKeys": ["payment_installments"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3_consumption_bucket",
)

job.commit()

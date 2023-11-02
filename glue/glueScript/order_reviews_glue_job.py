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


OrderReviweAWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database="ecommarce-database",
    table_name="olist_order_reviews_dataset_csv",
    transformation_ctx="OrderReviweAWSGlueDataCatalog",
)


OrderReviewChangeSchema = ApplyMapping.apply(
    frame=OrderReviweAWSGlueDataCatalog,
    mappings=[
        ("review_id", "string", "review_id", "string"),
        ("order_id", "string", "order_id", "string"),
        ("review_score", "long", "review_score", "long"),
        ("review_comment_title", "string", "review_comment_title", "string"),
        ("review_comment_message", "string", "review_comment_message", "string"),
        ("review_creation_date", "string", "review_creation_date", "string"),
        ("review_answer_timestamp", "string", "review_answer_timestamp", "string"),
    ],
    transformation_ctx="OrderReviewChangeSchema",
)


S3_consumption_bucket= glueContext.write_dynamic_frame.from_options(
    frame=OrderReviewChangeSchema,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://ecommarce-consumption-zone",
        "partitionKeys": ["review_score"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3_consumption_bucket=",
)

job.commit()

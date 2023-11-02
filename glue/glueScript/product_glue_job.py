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


ProductAWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database="ecommarce-database",
    table_name="olist_products_dataset_csv",
    transformation_ctx="ProductAWSGlueDataCatalog",
)


ProductChangeSchema = ApplyMapping.apply(
    frame=ProductAWSGlueDataCatalog,
    mappings=[
        ("product_id", "string", "product_id", "string"),
        ("product_category_name", "string", "product_category_name", "string"),
        ("product_name_lenght", "long", "product_name_lenght", "long"),
        ("product_description_lenght", "long", "product_description_lenght", "long"),
        ("product_photos_qty", "long", "product_photos_qty", "long"),
        ("product_weight_g", "long", "product_weight_g", "long"),
        ("product_length_cm", "long", "product_length_cm", "long"),
        ("product_height_cm", "long", "product_height_cm", "long"),
        ("product_width_cm", "long", "product_width_cm", "long"),
    ],
    transformation_ctx="ProductChangeSchema",
)

S3_consumption_bucket = glueContext.write_dynamic_frame.from_options(
    frame=ProductChangeSchema,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://ecommarce-consumption-zone",
        "partitionKeys": ["product_id"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3_consumption_bucket",
)

job.commit()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


SellerAWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(
    database="ecommarce-database", 
    table_name="olist_sellers_dataset_csv", 
    transformation_ctx="SellerAWSGlueDataCatalog")

SellerChangeSchema= ApplyMapping.apply(frame=SellerAWSGlueDataCatalog, mappings=[
    ("seller_id", "string", "seller_id", "string"), 
    ("seller_zip_code", "long", "seller_zip_code", "long"), 
    ("seller_city", "string", "seller_city", "string"), 
    ("seller_state", "string", "seller_state", "string")],
                                       transformation_ctx="SellerChangeSchema")


S3_consumption_bucket = glueContext.write_dynamic_frame.from_options(frame=SellerChangeSchema,
                                                                     connection_type="s3", 
                                                                     format="glueparquet",
                                                                     connection_options={"path": "s3://ecommarce-consumption-zone", "partitionKeys": ["seller_state"]},
                                                                     format_options={"compression": "snappy"}, 
                                                                     transformation_ctx="S3_consumption_bucket")

job.commit()
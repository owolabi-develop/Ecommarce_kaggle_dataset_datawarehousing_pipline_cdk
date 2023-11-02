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


OrderAWSGlueDataCatalog= glueContext.create_dynamic_frame.from_catalog(database="ecommarce-database", 
                                                                       table_name="olist_orders_dataset_csv", 
                                                                       transformation_ctx="OrderAWSGlueDataCatalog")


OrderChangeSchema= ApplyMapping.apply(frame=OrderAWSGlueDataCatalog, mappings=[
    ("col0", "string", "col0", "string"),
    ("col1", "string", "col1", "string"),
    ("col2", "string", "col2", "string"),
    ("col3", "string", "col3", "string"), 
    ("col4", "string", "col4", "string"), 
    ("col5", "string", "col5", "string"), 
    ("col6", "string", "col6", "string"), 
    ("col7", "string", "col7", "string")], 
                                      transformation_ctx="OrderAWSGlueDataCatalog")

S3_consumption_bucket = glueContext.write_dynamic_frame.from_options(frame=OrderChangeSchema, 
                                                                     connection_type="s3", format="glueparquet", 
                                                                     connection_options={"path": "s3://ecommarce-consumption-zone", "partitionKeys": []}, 
                                                                     format_options={"compression": "snappy"},
                                                                     transformation_ctx="S3_consumption_bucket")

job.commit()
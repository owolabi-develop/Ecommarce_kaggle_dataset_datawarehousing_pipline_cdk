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

AWSGlueDataCatalogcustomer = glueContext.create_dynamic_frame.from_catalog(database="ecommarce-database", 
                                                                           table_name="olist_customers_dataset_csv",
                                                                           transformation_ctx="AWSGlueDataCatalogcustomer")


ChangeSchemacustomer= ApplyMapping.apply(frame=AWSGlueDataCatalogcustomer, mappings=[
    ("customer_id", "string", "customer_id", "string"), 
    ("customer_unique_id", "string", "customer_unique_id", "string"), 
    ("customer_zip_code", "long", "customer_zip_code", "long"), 
    ("customer_city", "string", "customer_city", "string"), 
    ("customer_state", "string", "customer_state", "string")
    ], transformation_ctx="ChangeSchemacustomer")


s3_consumption_bucket= glueContext.write_dynamic_frame.from_options(frame=ChangeSchemacustomer, 
                                                                    connection_type="s3", format="glueparquet",
                                                                    connection_options={"path": "s3://ecommarce-consumption-zone",
                                                                                        "partitionKeys": ["customer_city", "customer_state"]},
                                                                    format_options={"compression": "snappy"}, transformation_ctx="s3_consumption_bucket")

job.commit()
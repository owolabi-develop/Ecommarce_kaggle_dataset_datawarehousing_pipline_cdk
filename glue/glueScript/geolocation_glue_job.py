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


AWSGlueDataCatalog_Geolocation = glueContext.create_dynamic_frame.from_catalog(database="ecommarce-database",
                                                                               table_name="olist_geolocation_dataset_csv", 
                                                                               transformation_ctx="AWSGlueDataCatalog_Geolocation"
                                                                               )


GeolocationChangeSchema = ApplyMapping.apply(frame=AWSGlueDataCatalog_Geolocation, mappings=[
    ("geolocation_zip_code", "long", "geolocation_zip_code", "long"), 
    ("geolocation_lat", "double", "geolocation_lat", "double"), 
    ("geolocation_lng", "double", "geolocation_lng", "double"), 
    ("geolocation_city", "string", "geolocation_city", "string"), 
    ("geolocation_state", "string", "geolocation_state", "string")
    ], transformation_ctx="GeolocationChangeSchema")


s3_consumption_bucket = glueContext.write_dynamic_frame.from_options(frame=GeolocationChangeSchema, 
                                                         connection_type="s3", 
                                                         format="glueparquet", 
                                                         connection_options={"path": "s3://ecommarce-consumption-zone", 
                                                                             "partitionKeys": ["geolocation_city", 
                                                                                               "geolocation_state"]}, 
                                                         format_options={"compression": "snappy"}, 
                                                         transformation_ctx="s3_consumption_bucket")

job.commit()
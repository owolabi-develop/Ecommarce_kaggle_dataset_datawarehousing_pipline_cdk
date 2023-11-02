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


OrderItemAWSGlueDataCatalog = glueContext.create_dynamic_frame.from_catalog(database="ecommarce-database", 
                                                                            table_name="olist_order_items_dataset_csv", 
                                                                            transformation_ctx="OrderItemAWSGlueDataCatalog")


OrderItemChangeSchema= ApplyMapping.apply(frame=OrderItemAWSGlueDataCatalog, mappings=[
    ("order_id", "string", "order_id", "string"), 
    ("order_item_id", "long", "order_item_id", "long"), 
    ("product_id", "string", "product_id", "string"), 
    ("seller_id", "string", "seller_id", "string"),
    ("shipping_limit_date", "string", "shipping_limit_date", "string"),
    ("price", "double", "price", "double"), 
    ("freight_value", "double", "freight_value", "double")
    ], transformation_ctx="OrderItemChangeSchema")


s3_consumption_bucket = glueContext.write_dynamic_frame.from_options(frame=OrderItemChangeSchema, 
                                                                          connection_type="s3", 
                                                                          format="glueparquet",
                                                                          connection_options={"path": "s3://ecommarce-consumption-zone", 
                                                                                              "partitionKeys": ["order_id"]}, 
                                                                          format_options={"compression": "snappy"}, 
                                                                          transformation_ctx="s3_consumption_bucket")

job.commit()
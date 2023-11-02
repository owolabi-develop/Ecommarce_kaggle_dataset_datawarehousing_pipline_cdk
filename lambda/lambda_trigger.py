import boto3





def handler(event,context):
    client = boto3.client('glue')
    
    glue_jobs = ['customer_glue_job',
            'geolocation_glue_job',
            "order_items_glue_job ",
            "order_payments_glue_job",
            "order_reviews_glue_job",
            "orders_glue_job",
            "product_glue_job",
            "seller_glue_job",
            "product_category_glue_job"
            ]
    for jobs in glue_jobs:
        response = client.start_job_run(
                JobName = jobs,
                )
    return response
    
    
    
    


---- customer table

CREATE TABLE ecommarce_sales.CUSTOMERS (
    customer_id VARCHAR,
    customer_unique_id varchar,
    customer_zip_code varchar,
    customer_city VARCHAR,
    customer_state varchar
);
create table ecommarce_sales.Geolocation (
    geolocation_zip_code VARCHAR,
    geolocation_lat GEOGRAPHY,
    geolocation_lng GEOGRAPHY,
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);
CREATE table ecommarce_sales.order_items (
    order_id VARCHAR,
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date DATE,
    price FLOAT4,
    freight_value FLOAT4
);
create table ecommarce_sales.order_payments (
    order_id VARCHAR,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value FLOAT4
);
create table ecommarce_sales.order_payments (
    review_id VARCHAR,
    order_id VARCHAR,
    review_score INT2,
    review_comment_title VARCHAR,
    review_comment_message VARCHAR,
    review_creation_date DATE,
    review_answer_timestamp TIMESTAMP
);
create table ecommarce_sales.orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE
);
CREATE table ecommarce_sales.product (
    product_id varchar,
    product_category_name varchar,
    product_name_lenght INT,
    product_description_lenght int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int
);
create table ecommarce_sales.sellers (
    seller_id VARCHAR,
    seller_zip_code INT2,
    seller_city VARCHAR,
    seller_state VARCHAR
);
create table ecommarce_sales.product_category (
    product_category_name varchar,
    product_category_name_english VARCHAR
);

----- load data from s3 lake to redshift table

copy ecommarce_sales.customers
    from 's3://ecommarce-raw-zones/olist_customers_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'


copy ecommarce_sales.geolocation
    from 's3://ecommarce-raw-zones/olist_geolocation_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'





copy ecommarce_sales.order_items
    from 's3://ecommarce-raw-zones/olist_order_items_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'





copy ecommarce_sales.order_payments
    from 's3://ecommarce-raw-zones/olist_order_payments_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'




copy ecommarce_sales.orders
    from 's3://ecommarce-raw-zones/olist_orders_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'



copy ecommarce_sales.product
    from 's3://ecommarce-raw-zones/olist_products_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'




copy ecommarce_sales.product_category
    from 's3://ecommarce-raw-zones/product_category_name_translation.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'


copy ecommarce_sales.orders_reviews
    from 's3://ecommarce-raw-zones/olist_order_reviews_dataset.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'




copy ecommarce_sales.sellers
    from 's3://ecommarce-raw-zones/product_category_name_translation.csv'
    format as csv
    delimiter ','
    quote '"'
    region 'us-east-1'
    IAM_ROLE 'arn:aws:iam::521427190825:role/service-role/AmazonRedshift-CommandsAccessRole-20240405T050528'

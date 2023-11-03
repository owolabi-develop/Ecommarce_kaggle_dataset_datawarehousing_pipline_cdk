
------- create database
create database Ecommarce_database


---- customer dataset table

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`customer_data` (
  `customer_id` string,
  `customer_unique_id` string,
  `customer_zip_code` int,
  `customer_city` string,
  `customer_state` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/customer_data'
TBLPROPERTIES ('classification' = 'csv');




--- create geolocation_dataset table

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`geolocation_dataset` (
  `geolocation_zip_code` int,
  `geolocation_lat` float,
  `geolocation_lng` float,
  `geolocation_city` string,
  `geolocation_state` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/geolocation_data/'
TBLPROPERTIES ('classification' = 'csv');




---- create order_item table

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`order_items_dataset` (
  `order_id` string,
  `order_item_id` int,
  `product_id` string,
  `seller_id` string,
  `shipping_limit_date` date,
  `price` decimal(1),
  `freight_value` decimal(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/order_items/'
TBLPROPERTIES ('classification' = 'csv');



--- create order_payment table
CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`order_payment` (
  `order_id` string,
  `payment_sequential` int,
  `payment_type` string,
  `payment_installments` int,
  `payment_value` decimal(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/order_payments/'
TBLPROPERTIES ('classification' = 'csv');




----- create order_review table 
CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`order_reviews` (
  `review_id` string,
  `order_id` string,
  `review_score` int,
  `review_comment_title` string,
  `review_comment_message` string,
  `review_creation_date` date,
  `review_answer_timestamp` timestamp
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/order_review/'
TBLPROPERTIES ('classification' = 'csv');




---- create orders table 

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`orders` (
  `order_id` string,
  `customer_id` string,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `order_approved_at` date,
  `order_delivered_carrier_date` date,
  `order_delivered_customer_date` date,
  `order_estimated_delivery_date` date
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/orders/'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);


--- create  product table


CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`products` (
  `product_id` string,
  `product_category_name` string,
  `product_name_lenght` int,
  `product_description_lenght` int,
  `product_photos_qty` int,
  `product_weight_g` int,
  `product_length_cm` int,
  `product_height_cm` int,
  `product_width_cm` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escape.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/products/'
TBLPROPERTIES ('classification' = 'csv');


--- create seller table

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`sellers` (
  `seller_id` string,
  `seller_zip_code` int,
  `seller_city` string,
  `seller_state` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/sellers/'
TBLPROPERTIES ('classification' = 'csv');



--- create product category table

CREATE EXTERNAL TABLE IF NOT EXISTS `ecommarce_database`.`product_category` (
  `product_category_name` string,
  `product_category_name_english` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'escaped.by' = '" "')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ecommarce-raw-zones/product_cat/'
TBLPROPERTIES ('classification' = 'csv');



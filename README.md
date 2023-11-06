
# Ecommarce kaggle Dataset data warehousing pipline


## Architechure Diagram
![alt text](digramphoto/aws_digram_proj_4_data_warehousing.png)

## Dataset schema design 
![alt text](<digramphoto/database design.png>)

## Sample ecommarce Data on S3 data lake
![alt text](digramphoto/samples_folders.jpg)

## Redshift  cluster warehouse table creation
![alt text](digramphoto/redshift_tables.jpg)

## Seller with the Highest Sales order Sample Query
![alt text](digramphoto/seller_heiedh.jpg)

## Product with Highest Sales
![alt text](digramphoto/product_heiesh.jpg)


```
$ python -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

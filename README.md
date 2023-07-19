# aws-Kinesis-GlueSchema validator
This code produces data to the Kinesis producer by using AWS Glue Schema using AVRO-encoding messages .The AVRO message will read a Glue Schema in AWS and validate the message before sending this to Kinesis Producer

The code contains the cloudformation templates which will create the Kineis Data stream , Glue Registry , Glue Schema and a Lambda Function which uses the AVRO library to read the Glue Schema and validate the data before sending this to Kinesis Data Producer. 

The Lambda code is written in Python and the source code is available in the Python folder , this is pacakaged as a zip packagae with the dependency. The code uses boto3 and avro librray to read the schema and use the avro encoder to validate the data before publishing it to the Kinesis Producer. Python folder contain the Lambdacode in the file named lambda_function.py . If you cahnge the source code you can create the zip package ,  the directory is named python . Navigate to the Project directory 
  cd Python

Create a new directory named package  install the avro dependency
  mkdir package
  pip install --target ./package avro-python3==1.8.2 

Create a .zip file with the installed libraries at the root project
  cd package
  zip -r ../my_deployment_package.zip .

Add the lambda_function.py file to the root of the .zip file
  cd ..
  zip my_deployment_package.zip lambda_function.py

Upload the .zip package to the S3 Bucket and pass the reference to the cloudformation template , cloudformation template will create the lambda function from the zip pacakge.

  
Upload Template Files from the cfn folder to S3 Bucket:

aws s3 cp . s3://my-bucket/cfntemplate.yaml/ --recursive

CloudFormation Stack Creation:

aws cloudformation create-stack \
--stack-name datavalidator \
--template-url https://my-bucket.s3.region.amazonaws.com/datavalidator/cfntemplate.yaml \
--capabilities CAPABILITY_NAMED_IAM
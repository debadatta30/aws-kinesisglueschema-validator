# aws-Kinesis-GlueSchema validator
This code produces data to the Amazon Kinesis producer by using AWS Glue Schema using AVRO-encoding messages .The AVRO message will use the Glue Schema to validate the message before sending this to Amazon Kinesis Producer . The below diagram shows the flow sequnce of the data validation before producing the data to Amazon Kinesis Data Stream

<img width="320" alt="image" src="https://github.com/debadatta30/aws-kinesisglueschema-validator/assets/136390466/d9c2baa9-96ae-4d59-a1fa-f8d528f52129">


The code contains the cloudformation templates which will create the Amazon Kineis Data stream , AWS Glue Registry , AWS Glue Schema and a AWS Lambda Function which uses the AVRO library to read theAWS Glue Schema and validate the data before sending this to Kinesis Data Producer. 

The AWS Lambda code is written in Python and the source code is available in the Python folder , this is pacakaged as a zip packagae with the dependency. The code uses boto3 and avro librray to read the schema and use the avro encoder to validate the data before publishing it to the Kinesis Producer. Python folder contain the Lambdacode in the file named lambda_function.py . If you cahnge the source code you can create the zip package ,  the directory is named python . Navigate to the Project directory 
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

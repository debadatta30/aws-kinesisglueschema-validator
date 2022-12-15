import json
import boto3
import avro.schema
import avro.io
import io
import logging
import os

# Set up the  logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Env variables from  CFN 
streamName = os.environ.get('streamName')
schName = os.environ.get('schName')
regName = os.environ.get('regName')

kinesis = boto3.client('kinesis')
glue = boto3.client('glue')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    try:
        schema_res = glue.get_schema_version( SchemaId = { 'SchemaName': schName , 'RegistryName': regName },SchemaVersionNumber={'VersionNumber': 1})
        schema_defination = schema_res['SchemaDefinition']
        #print(schema_defination)
        logger.info(schema_defination)
        schema = avro.schema.Parse(schema_defination)
        writer = avro.io.DatumWriter(schema)
        
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write({"Order": "Apple", "Quantity": 256}, encoder)
        
        raw_bytes = bytes_writer.getvalue()
        response =  kinesis.put_record( StreamName= streamName,Data=raw_bytes,PartitionKey="1")
        logger.info(response)
    except Exception as e:
        #print(e)
        logger.info(e)
        raise e
        
        

import logging
import time
import boto3

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

log = logging.getLogger()
log.setLevel(logging.INFO)


def lambda_handler(event, context):
    print('hello')
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        log.info(f'bucket {bucket}')
        log.info(f'key {key}')




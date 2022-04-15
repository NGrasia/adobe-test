#!/usr/bin/env python3
# encoding: utf-8

"""
lambda_function.py
my-sam-app/lambdas/lambda_function.py

created by Nilesh Grasia on 04/15/2022

Lambda function will read file from s3 bucket and perfrom analytics operation
and output file is generated and placed into another bucket for customer analysis .


Version 1.0
04/15/2022
"""
import csv
import json
import datetime
import logging
import boto3
import pandas as pd

log = logging.getLogger()
log.setLevel(logging.INFO)


class FileProcess():

    def __init__(self,event) -> None:
        """Object will take event from lambda handler,expects the event to be a dictionary with
        following keys : bucket,key,file object

        Args:
            ""event(dict): passed in from the lanbda_handler """

        self._bucket = event
        self._key =""
        self._file = ""


    def _filename_format(self):
        """[Date]_SearchKeywordPerformance.tab
        [Date] corresponds to the date the application executed for
        The format should be YYYY-mm-dd (i.e. 2009-10-08)"""
        pass



    def _write_tsv(self,data):
        """ This function reads tsv file and select required details ,process and write the result
        set into another tsv file """

        current_date = datetime.date.today()
        output_filename = current_date + "_SearchKeywordPerformance.tab"
        log.info(f"Filename : {output_filename}")

        tsv_data = pd.read_csv(data, sep='\t')
        print(tsv_data.head())

        with open(output_filename,"wt",delimiter="\t") as tsv_writer:
            for line in tsv_data:
                tsv_writer.writerow(line)
            log.info(f"line :{line}")
        # s3_object.upload_file('/tmp/test.csv', key)



def lambda_handler(event, context):

    """Triggered from landing S3 bucket on object create or copied
        Tha lambda function will process any new file landed in S3 bucket
        File : TSV format

        Args :
            event(dict):
            context(object): """
    log.info("Starting Lambda processing")
    log.info(f"Event: {event}")
    log.info(f"Context: {context}")
    response = {"code": 400, "message": ""}
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        source_path = event['Records'][0]['s3']['object']['key']
        log.info(f"Input bucket name: {source_bucket}")
        log.info(f"Input key name : {source_path}")
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(source_bucket, source_path)
        data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        cls_obj = FileProcess()
        cls_obj._write_tsv(data)
        response["code"] = 200
        response["message"] = f"Completed file processing and placed here:"
        log.info("File processing is completed")
    except Exception as e:
        log.info(f"Unexpected Error : {e}")
    finally:
        log.critical(response)
        return response


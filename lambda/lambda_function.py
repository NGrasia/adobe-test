#!/usr/bin/env python3
# encoding: utf-8

"""
lambda_function.py
my-sam-app/lambda/lambda_function.py

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
import urllib.parse as url
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
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


    def _begin_parsing(self):
        """This function takes file data and create dataframe using pandas lib
        also parse the required columns to achieve client requirments"""

        data = pd.read_csv("data.tsv", sep="\t")
        df = pd.DataFrame(data)
        df1 = df['product_list'].str.split(';', expand=True)
        df1.columns = ["category", "product_name", "nos_items", "total_revenue", "custom_events"]
        df2 = pd.concat([df, df1], axis=1)
        self._find_keyword_search_url(df2)
        log.info(f"keyword function completed")
        self._dataframe_query()
        log.info(f"query function completed")
        self._write_file()
        log.info(f"write function completed")


    def _dataframe_query(self):
        #TODO
        """ This function runs sql query on final dataframe and send result to write output tsv file"""

        q = """SELECT   seach_engine as search_engine_domain,
                        keyword as search_keyword
                        sum(total_revenue) as revenue
               FROM df2 WHERE event_list in (2,1) 
               and search_engine_domain not in ("www.esshopzilla.com")
               group by search_engine_domain,search_keyword order by revenue desc
               LIMIT 5;"""
        names = pysqldf(q)
        log.info(f"query output: {names}")

    def _write_file(self):
        """This function reads tsv file and select required details ,process and write the result
        set into another tsv file """

        current_date = datetime.date.today()
        output_filename = current_date + "_SearchKeywordPerformance.tab"
        log.info(f"Filename : {output_filename}")

        self.names.to_csv(output_filename, sep='\t',index=False)
        log.info(f"Writing file has completed")

    def _find_keyword_search_url(self,df2):

        #TODO This function can be imporved with better webscraping functionality to get keywords.

        """ This function takes input dataframe and process url column to get keyword and respective
            search engine url using urllib library
            Args: Dataframe

            Returns: Added two columns keyword and search engine url
        """

        url = df2["referrer"]
        keyword = []
        search_engine = []

        for i in url:
            if ('google' in i) or ('bing' in i):#google and bing use q query string
                pr = url.urlparse(i)
                qs = url.parse_qs(pr.query)['q']
                log.info(f"keyword: {qs}")
                search_engine.append(pr[1])
                keyword.extend(qs)
            elif 'yahoo' in i: #as yahoo seach engine uses p query string
                pr = url.urlparse(i)
                qs = url.parse_qs(pr.query)['p']
                log.info(f"keyword: {qs}")
                search_engine.append(pr[1])
                keyword.extend(qs)
            else:
                pr = url.urlparse(i)
                keyword.append("None")
                search_engine.append(pr[1])
        df2["search_engine"] = search_engine
        df2["keyword"] = keyword
        return df2


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


#!/usr/bin/env python3
# encoding: utf-8

"""
lambda_function.py
code/lambda/lambda_function.py

created by Nilesh Grasia on 04/17/2022

Lambda function will read file from s3 bucket and perform analytics operation
and output file is generated and placed into another bucket for customer analysis .


Version 1.0
04/17/2022
"""
import csv
import json
import datetime
import logging
import boto3
import pandas as pd
import urllib.parse as url
from pandasql import sqldf
import io
import os

# pysqldf = lambda q: sqldf(q, globals())

log = logging.getLogger()
log.setLevel(logging.INFO)


class FileProcess:

    def __init__(self, source_bucket, source_path) -> None:
        """Object will take event from lambda handler,expects the event to be a dictionary with
        following keys : bucket,key,file object

        Args:
            ""event(dict): passed in from the lanbda_handler """

        self._bucket = source_bucket
        self._key = source_path
        self._begin_parsing()


    def _begin_parsing(self):
        """This function takes file data and create dataframe using pandas lib
        also parse the required columns to achieve client requirments"""

        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        obj = s3_resource.Object(self._bucket, self._key)
        for file in s3_resource.Bucket(name=self._bucket).objects.filter(Prefix='sample_files/'):
            filename = file.key.split('/')[-1]
        # data = obj["Key"].split("/")[-1]
        log.info(f"data : {filename}")

        response = s3_client.get_object(Bucket=self._bucket, Key=self._key)

        #All boto3 connecttion can be set up in separate fucntion

        data = pd.read_csv(response.get("Body"), sep="\t")
        df = pd.DataFrame(data)
        log.info(f"df: {df}")

        #spliting product list to get other additional values
        df1 = df['product_list'].str.split(';', expand=True)
        log.info(f"df1: {df1}")
        df1.columns = ["category", "product_name", "nos_items", "total_revenue", "custom_events"]
        df2 = pd.concat([df, df1], axis=1)

        #Created search keyword thru urllib parse also can be achieved web scrabing libs.
        self._find_keyword_search_url(df2)
        log.info(f"keyword function completed")

        #Utlizing pandasql to execute sql queries on dataframe
        #Another approaches withing dataframe methods
        self._names,self._names_a = self._dataframe_query(df2)
        log.info(f"query function completed")

        #Finally writing output result to s3 bucket.
        self._write_file(self._names,self._names_a)
        log.info(f"write function completed")

    def _dataframe_query(self, df2):
        # TODO
        """ This function runs sql query on final dataframe and send result to write output tsv file"""

        log.info(f"df2 : {df2}")

        q = """SELECT  search_engine as search_engine_domain,
                        keyword as search_keyword,
                        sum(total_revenue) as revenue
               FROM df2 WHERE event_list in (2,1) 
               -- and search_engine_domain not in ("www.esshopzilla.com")
               group by search_engine_domain,search_keyword order by revenue desc
              """
        qa = """SELECT  referrer,product_list,event_list,
                                category,search_engine,keyword,product_name,nos_items,
                                total_revenue,custom_events
                            FROM df2 order by total_revenue desc;
                      """
        names = sqldf(q)
        names_a =sqldf(qa)

        log.info(f"query output: {names}")
        return names,names_a

    def _write_file(self,names,names_a):
        """This function reads tsv file and select required details ,process and write the result
        set into another tsv file """

        self._names= names
        self._names_a = names_a

        current_date = datetime.date.today()
        output_filename = str(current_date) + "_SearchKeywordPerformance.tab"
        output_filename_detail = str(current_date) + "_SearchKeywordPerformance_Detailed.tab"
        log.info(f"Filename : {output_filename}")

        s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        obj = s3_resource.Object(self._bucket,output_filename)

        # self._names.to_csv(obj.put(), sep='\t', index=False)
        # log.info(f"Writing file has completed")

        with io.StringIO() as csv_buffer:
            self._names.to_csv(csv_buffer,sep='\t', index=False)

            response = s3_client.put_object(
                Bucket=self._bucket, Key=f"daily_output/{output_filename}", Body=csv_buffer.getvalue()
            )

        with io.StringIO() as csv_buffer:
            self._names_a.to_csv(csv_buffer,sep='\t', index=False)

            response = s3_client.put_object(
                Bucket=self._bucket, Key=f"daily_output_detailed/{output_filename_detail}", Body=csv_buffer.getvalue()
            )

        log.info(f"Writing file has completed..")

    def _find_keyword_search_url(self, df2):

        # TODO This function can be imporved with better webscraping functionality to get keywords.

        """ This function takes input dataframe and process url column to get keyword and respective
            search engine url using urllib library
            Args: Dataframe

            Returns: Added two columns keyword and search engine url
        """

        urlf = df2["referrer"]
        keyword = []
        search_engine = []
        log.info(f"url: {url}")

        for i in urlf:
            if ('google' in i) or ('bing' in i):  # google and bing use q query string
                pr = url.urlparse(i)
                qs = url.parse_qs(pr.query)['q']
                log.info(f"keyword: {qs}")
                search_engine.append(pr[1])
                keyword.extend(qs)
            elif 'yahoo' in i:  # as yahoo seach engine uses p query string
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

        cls_obj = FileProcess(source_bucket, source_path)
        cls_obj._begin_parsing()

        # cls_obj = FileProcess()
        # cls_obj._begin_parsing(file)
        response["code"] = 200
        response["message"] = f"Completed file processing and placed here:"
        log.info("File processing is completed")
    except Exception as e:
        log.info(f"Unexpected Error : {e}")
    finally:
        log.critical(response)
        return response


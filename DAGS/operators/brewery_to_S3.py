## brewery_to_s3.py  => operator

import logging
import pandas as pd
import json
import boto3
import requests
import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from airflow.operators.python import get_current_context


"""The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift.
The operator creates and runs a SQL COPY statement based on the parameters provided.
The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file.
Another important requirement of the stage operator is containing a templated field that allows it to load
timestamped files from S3 based on the execution time and run backfills."""

class BreweryToS3Operator(BaseOperator):
    template_fields = ('link_file','file_name',)

    @apply_defaults
    def __init__(self,
        aws_credentials="",
        bucket_name="",
        link_file="",
        file_name="",
        per_page="",
        *args, **kwargs):

        super(BreweryToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.bucket_name=bucket_name
        self.link_file=link_file
        self.file_name = file_name        
        self.per_page=per_page
        

    def execute(self, context):
        params = context['task_instance'].xcom_pull(task_ids='brewery_metadata', key='page')
        execution_date = context['execution_date']
        execution_date = execution_date.date().strftime('%Y-%m-%d')
        total_page = params[0]
        per_page = params[1]
               
        logging.info(f"Parameters: {self.aws_credentials}, {self.bucket_name}, \
                     {self.link_file}, {self.file_name}, {total_page}, {per_page}, {execution_date}")
        connection = BaseHook.get_connection(self.aws_credentials)
        
        secret_key = connection.password # This is a getter that returns the unencrypted pass
        access_key = connection.login # This is a getter that returns the unencrypted login

        bucket = Variable.get(self.bucket_name)

        file_path = self.link_file
        logging.info(file_path)
        
        #Creating Session With Boto3 and Creating S3 Resource From the Session.
        session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='us-east-1')
        s3 = session.resource('s3')

        
        for page in range(1, total_page + 1):
            link_file=file_path.format(page, per_page) 
            request = requests.get(link_file)
            
            if request.status_code != 200:
                logging.warning(f"Bad request {request.status_code} - {request.json()} for file {link_file}")
            else:
                if len(request.json()) == 0:
                    logging.warning(f"No data in file {link_file}")
            
            file_name = self.file_name.format(page,execution_date)
            s3_object = s3.Object(bucket, file_name)
            file_data = request.text
            s3_insert = s3_object.put(Body=file_data)      

            result = s3_insert.get('ResponseMetadata')

            if result.get('HTTPStatusCode') == 200:
                logging.info(f'File Uploaded Successfully {self.file_name}')
            else:
                logging.error(f'File Not Uploaded {self.file_name}')

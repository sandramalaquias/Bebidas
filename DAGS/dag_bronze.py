
## BEES DAG

from datetime import datetime, timedelta
import os
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators import BreweryToS3Operator
from airflow.operators.python import task, get_current_context
from airflow.operators.dummy_operator import DummyOperator

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'smm',
    'start_date': datetime.now(),
    'schedule_interval': None, 
    'depends_on_past': False,
    'catchup':False,
   }

dag = DAG('brewery_file_to_s3',
          default_args=default_args,
          description='Load data into S3 Airflow',
          tags=["bees"]
        ) 

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)
            
def brewery_metadata(*args, **kwargs):
    context = get_current_context()
    ts = datetime.fromisoformat(context['ts'])
    
    link_file="https://api.openbrewerydb.org/v1/breweries/meta"

    metadata_response = requests.get(link_file)   
    if metadata_response.status_code != 200:
        raise ValueError(f"Error {metadata_response.json()} for file {link_file}")

    metadata = metadata_response.json()    
    if int(metadata['total']) == 0:
        raise ValueError(f"Error No page in metadata for file {link_file}")
        
    if int(metadata['total']) % 200 == 0:
        total_pages = int(metadata['total']) / 200
    else:
        total_pages = int((int(metadata['total']) / 200) + 1)

    ti = kwargs['ti']
    ti.xcom_push(key='page', value=[total_pages, 200])
          
get_page = PythonOperator(
     task_id = 'brewery_metadata',
     python_callable = brewery_metadata,
     provide_context = True,
     dag = dag
     ) 

create_brewery_file= BreweryToS3Operator(
        task_id="brewery_file",
        dag=dag,
        aws_credentials="my_credentials",
        bucket_name="brewery_bucket",
        link_file='https://api.openbrewerydb.org/v1/breweries?page={}&per_page={}',
        file_name='bronze/brewery.{}.{}.json'
         )

start_operator >> get_page >> create_brewery_file >> end_operator


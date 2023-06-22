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
from airflow.operators.bash_operator import BashOperator

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'smm',
    'start_date': datetime.now(),
    'schedule_interval': None, 
    'depends_on_past': False,
    'catchup':False,
   }

dag = DAG('brewery_silver',
          default_args=default_args,
          description='Load data into S3 as parquet',
          tags=["bees"]
        ) 

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)
            

task_silver = BashOperator(
        dag=dag,
        task_id='load_silver',
        bash_command="python $AIRFLOW_HOME/dags/sparkSilver.py --configpath $AIRFLOW_HOME/dags/aws.cfg",
    )

start_operator >> task_silver >> end_operator


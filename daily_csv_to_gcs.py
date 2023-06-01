from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from airflow.operators.dummy_operator import DummyOperator

import pandas as pd
from datetime import timedelta, datetime
from airflow.models import Variable
from functions import get_data, upload_to_gcs 
import os

default_args = {
    'owner': 'Diego Gonzalez',
    'start_date': datetime(2023, 5, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup':True,
    'schedule_interval':None
}

api_key = Variable.get("API_KEY")
local_path = Variable.get("local_path")

with DAG(
    'daily_csv_to_gcs',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    run_script_task = PythonOperator(
        task_id='run_script_task',
        python_callable=get_data,
        op_args=[api_key]
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> run_script_task >> upload_to_gcs_task >> end

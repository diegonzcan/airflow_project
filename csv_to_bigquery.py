from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'Diego Gonzalez',
    'start_date': datetime(2023, 5, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':None
}

with DAG('csv_to_bigquery', default_args=default_args, schedule_interval=None) as dag:
        
    hoy = date.today()
    hoy_str = hoy.strftime("%Y-%m-%d")
    file = f'raw_{hoy_str}.csv' 

    project = 'pipeline-387723' 
    table = 'forex'  
    dataset = 'exchange_rates'

    upload_csv = GoogleCloudStorageToBigQueryOperator(
        task_id='upload_csv',
        bucket='raw_bucket_diego',
        source_objects=[f'gs:/raw_bucket_diego/{file}'],
        destination_project_dataset_table=f'{project}.{dataset}.{table}',
        schema_fields=[
            {'name': 'fr_curr_cd', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'fr_curr_nm', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'to_curr_cd', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'to_curr_nm', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'exchange_rt', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'last_refreshed_dt', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'tz', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bid_pr', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'ask_pr', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND',  
        skip_leading_rows=1 
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> upload_csv >> end

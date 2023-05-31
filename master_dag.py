from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date': datetime(2023, 5, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'0 6 * * *'

}

with DAG(
    'master_dag',
    default_args=default_args,
    catchup=False,
) as dag:
    trigger_daily_csv_to_gcs = TriggerDagRunOperator(
        task_id='trigger_daily_csv_to_gcs',
        trigger_dag_id='daily_csv_to_gcs',
    )

    trigger_csv_to_bigquery = TriggerDagRunOperator(
        task_id='trigger_csv_to_bigquery',
        trigger_dag_id='csv_to_bigquery',
    )

    trigger_daily_csv_to_gcs >> trigger_csv_to_bigquery

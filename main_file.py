import os
import logging
import airflow
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import storage
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

args = {
    'owner': 'shashidhar'
}

dag = DAG(
    dag_id='Gcs_to_bq_load',
    default_args=args,
    schedule_interval=None,
    tags=['shashi', 'dhar', 'data'],
    catchup=False
)

schemas = [
    {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'column2', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'column3', 'type': 'STRING', 'mode': 'NULLABLE'},
]

def read_csv_file(bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    source_file = f'/tmp/{file_name}'
    blob.download_to_filename(source_file)
    
    df = pd.read_csv(source_file)
    df_columns = df.columns.tolist()
    logging.info(f"Columns in the file: {df_columns}")

    schema_columns = [field['name'] for field in schemas]
    logging.info(f"Schema columns: {schema_columns}")

    if set(df_columns) == set(schema_columns):
        logging.info("Column names match the schema.")
    else:
        logging.warning("Column names do not match the schema.")
    
    return df_columns

task1 = DummyOperator(task_id='start', dag=dag)
task2 = DummyOperator(task_id='end', dag=dag)

open_file = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv_file,
    op_kwargs={'bucket_name': 'shashidhar_bucket', 'file_name': 'sample.csv'},
    dag=dag
)

gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_to_bq_load',
    bucket='shashidhar_bucket',
    source_objects=['sample.csv'],
    destination_project_dataset_table='charged-magnet-451705-u5.shashi_dataset.gcs_to_bq_table',
    schema_fields=schemas,
    skip_leading_rows=1,
    create_disposition='CREATE_IF_NEEDED',
    execution_timeout=timedelta(minutes=5),
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

task1 >> open_file >> gcs_to_bq_load >> task2

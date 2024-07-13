from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'upload_from_url_to_s3',
    default_args=default_args,
    description='Download dataset from URL and directly upload to S3',
    schedule_interval='@once',
)

def get_aws_credentials(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return conn.login, conn.password

def upload_file_to_s3(url, s3_bucket, s3_key, aws_conn_id, region_name='ap-southeast-3'):
    access_key, secret_key = get_aws_credentials(aws_conn_id)

    # Download the file content from URL
    response = requests.get(url)
    response.raise_for_status()  # Ensure we got a valid response

    # Upload to S3 using boto3
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=response.content)
        print(f"Upload successful: s3://{s3_bucket}/{s3_key}")
    except NoCredentialsError:
        print("Credentials not available")

def download_and_upload_to_s3():
    url = 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-01-21/spotify_songs.csv'
    s3_bucket = 'intermediary-bucket-house'
    s3_key = 'spotify_songs/spotify_songs.csv'
    aws_conn_id = 'aws_conn'

    upload_file_to_s3(url, s3_bucket, s3_key, aws_conn_id)

upload_task = PythonOperator(
    task_id='download_and_upload_to_s3',
    python_callable=download_and_upload_to_s3,
    dag=dag,
)

upload_task


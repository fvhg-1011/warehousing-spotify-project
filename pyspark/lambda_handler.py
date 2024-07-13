import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

#variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
redshift_host = os.getenv('REDSHIFT_HOST')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_dbname = os.getenv('REDSHIFT_DBNAME')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
bucket_name = os.getenv('S3_BUCKET_NAME')
file_key = os.getenv('FILE_KEY')
s3_object_url = f's3://{bucket_name}/{file_key}'
redshift_table = 'spotify_songs'

def copy_s3_to_redshift():
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password
        )
        cursor = conn.cursor()

        # S3 to redshift
        copy_command = f"""
        COPY {redshift_table}
        FROM '{s3_object_url}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        CSV
        IGNOREHEADER 1;
        """

        # exec command
        cursor.execute(copy_command)
        conn.commit()

        # close connection
        cursor.close()
        conn.close()

        print("Data loaded successfully")

    except Exception as e:
        print(f"Error: {str(e)}")

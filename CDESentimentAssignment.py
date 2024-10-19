## Step 1
import airflow 
import requests, zipfile
from airflow import DAG 
from airflow.utils.dates import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# step 2
#Instantiate a DAG object; 
# this is the starting point of any workflow
with DAG(
    dag_id= "CDESentiment",  #The name of the DAG
    start_date= datetime(2024, 10, 16), #The date at which the DAG should first start running
    schedule_interval=None,  #At what interval the DAG should run
    catchup=False,
    #tag="Assignment"
) as dag:
    
    # step 3: python function to download a zip file 
    # downloading 11pm pageviews data for 15th of October, 2024
    
    def _download_zipped_file(url, save_path):
        url = 'https://dumps.wikimedia.org/other/pageviews/2024/2024-10/pageviews-20241015-140000.gz'
        save_path = 'dags/Assignment/ExtractedFiles.zip'       
        zipped_files = requests.get(url) # Send a GET request to the pageviews URL
        with open('save_path', 'wb') as f:  # Open the file in binary write mode and save the content as a json file
            f.write(zipped_files.content)

        print(f"File downloaded successfully and saved to: {save_path}")
        
    # step 4    
    # python operator for the function           
    download_zipped_files = PythonOperator(
    task_id = 'download_zipped_files',
    python_callable= _download_zipped_file
    )
    
    # step 5 
    # Task to extract the zip file
    extract_zip_files = BashOperator(
        task_id='extract_zip_files',
        bash_command= 'dags/Assignment/ExtractedFiles.zip -d dags/Assignment/ExtractedFiles' 
    )
    
    # step 6 
    #Function to fetch pageviews and write to SQL file
    def _fetch_pageviews():
        
        # Logic to read extracted data and write to SQL
        print ("pageviews fetched successfully to SQL db")
    
    # Step 7
    # Python operator for fetching pageviews
    fetch_pageviews = PythonOperator(
    task_id='fetch_pageviews',
    python_callable= _fetch_pageviews
    )
    
    # Step 8: Database operator to load data (example for Postgres)
    postgres_data_load = BashOperator(
    task_id='postgres_data_load',
    bash_command='psql -U user -d database -f /path/to/sql_file.sql'
    )
    
    # Step 9: Set task dependencies
    download_zipped_files >> extract_zip_files >> fetch_pageviews >> postgres_data_load
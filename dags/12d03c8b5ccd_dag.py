from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta 

# Define params for the Submit Run Operator
notebook_task = {
    'notebook_path': '/Workspace/Users/carla.costan0@gmail.com/mount_S3',  
}

# Define default arguments for the DAG
default_args = {
    'owner': 'Carla',  
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  
    'retry_delay': timedelta(minutes=2),
}

# Instantiate the DAG
with DAG('databricks_dag',
    start_date=datetime(2024, 8, 1),  
    schedule_interval='@daily',  
    catchup=False,
    default_args=default_args
    ) as dag:

    # Define the task to submit a run to Databricks
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',  
        existing_cluster_id='1108-162752-8okw8dgg',  
        notebook_task=notebook_task
    )

    
    opr_submit_run

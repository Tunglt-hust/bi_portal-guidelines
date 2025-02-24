from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False
}

# Create DAG
with DAG(
    'create_bigquery_dataset',
    default_args=default_args,
    description='DAG to execute BigQuery dataset creation script',
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['bigquery']
) as dag:

    # Task to execute the Python script
    execute_script = BashOperator(
        task_id='create_dataset_in_bigquery',
        bash_command='python {{ var.value.scripts_path }}/dataset_creator.py',
        dag=dag
    )

    # You can add more tasks here if needed
    
    # Set task dependencies
    execute_script

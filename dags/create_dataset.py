from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Create DAG
dag = DAG(
    'create_bigquery_dataset',
    default_args=default_args,
    catchup=False,
)

# Push data to BigQuery using a bash command
create_dataset_task = BashOperator(
    task_id='create_dataset_in_bigquery',
    bash_command='python /home/airflow/scripts/dataset_creator.py',
    dag=dag,
)

create_dataset_task

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
    'run_bigquery_query',
    default_args=default_args,
    catchup=False,
)

# Push data to BigQuery using a bash command
run_query_task = BashOperator(
    task_id='push_data_to_bigquery',
    bash_command='python /home/airflow/scripts/query_runner.py',
    dag=dag,
)

run_query_task

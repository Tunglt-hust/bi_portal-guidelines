from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "create_dataset",
    default_args=default_args,
    catchup=False,
)

# Task to create a BigQuery dataset using bq command
create_dataset = BashOperator(
    task_id="create_dataset",
    bash_command="""
    bq --location=US mk --dataset \
    --description 'Dataset for BI Portal' \
    your_project_id:your_dataset_name
    """,
    dag=dag,
)

if __name__ == "__main__":
    create_dataset

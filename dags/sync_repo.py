from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# GitHub repo and Airflow paths
GITHUB_REPO = "git@github.com:user/repo.git"
LOCAL_REPO_PATH = "/home/airflow/repo"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "sync_repo",
    default_args=default_args,
    catchup=False,
)

# Clone or pull the latest repo changes
sync_repo = BashOperator(
    task_id="clone_or_pull_repo",
    bash_command=f"""
    if [ -d {LOCAL_REPO_PATH} ]; then
        cd {LOCAL_REPO_PATH} && git reset --hard && git pull origin main
    else
        git clone {GITHUB_REPO} {LOCAL_REPO_PATH}
    fi
    """,
    dag=dag,
)

# You can add more tasks if needed!

if __name__ == "__main__":
    dag.cli()

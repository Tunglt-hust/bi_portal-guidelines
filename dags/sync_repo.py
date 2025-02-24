from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# GitHub and Airflow configurations
GITHUB_REPO = "git@github.com:user/repo.git"
LOCAL_REPO_PATH = "/home/airflow/repo"
BRANCH = "main"  # Default branch to sync

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

def check_repo_exists():
    """Check if repository exists locally and return appropriate task."""
    if os.path.exists(LOCAL_REPO_PATH):
        return "pull_repo"
    return "clone_repo"

# Define the DAG
with DAG(
    "github_sync",
    default_args=default_args,
    description="Sync GitHub repository with local filesystem",
    schedule_interval="0 */6 * * *",  # Run every 6 hours
    catchup=False,
    tags=['github', 'sync'],
    max_active_runs=1
) as dag:

    # Check repository state
    check_repo = BranchPythonOperator(
        task_id="check_repo_state",
        python_callable=check_repo_exists
    )

    # Clone repository if it doesn't exist
    clone_repo = BashOperator(
        task_id="clone_repo",
        bash_command=f"""
        if [ ! -d {LOCAL_REPO_PATH} ]; then
            git clone -b {BRANCH} {GITHUB_REPO} {LOCAL_REPO_PATH}
            if [ $? -eq 0 ]; then
                echo "Repository cloned successfully"
            else
                echo "Failed to clone repository"
                exit 1
            fi
        else
            echo "Directory already exists"
            exit 1
        fi
        """
    )

    # Pull latest changes if repository exists
    pull_repo = BashOperator(
        task_id="pull_repo",
        bash_command=f"""
        if [ -d {LOCAL_REPO_PATH} ]; then
            cd {LOCAL_REPO_PATH}
            
            # Stash any local changes
            git stash
            
            # Reset to head
            git reset --hard HEAD
            
            # Clean untracked files
            git clean -fd
            
            # Pull latest changes
            git pull origin {BRANCH}
            
            if [ $? -eq 0 ]; then
                echo "Repository updated successfully"
            else
                echo "Failed to update repository"
                exit 1
            fi
        else
            echo "Repository directory not found"
            exit 1
        fi
        """
    )

    # Verify repository state after sync
    verify_sync = BashOperator(
        task_id="verify_sync",
        bash_command=f"""
        cd {LOCAL_REPO_PATH}
        
        # Check if we're on the correct branch
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
        if [ "$CURRENT_BRANCH" != "{BRANCH}" ]; then
            echo "Wrong branch: $CURRENT_BRANCH (expected: {BRANCH})"
            exit 1
        fi
        
        # Check for any uncommitted changes
        if [ -n "$(git status --porcelain)" ]; then
            echo "Repository has uncommitted changes"
            exit 1
        fi
        
        # Check if we're up to date with remote
        git fetch origin {BRANCH}
        LOCAL_COMMIT=$(git rev-parse HEAD)
        REMOTE_COMMIT=$(git rev-parse origin/{BRANCH})
        if [ "$LOCAL_COMMIT" != "$REMOTE_COMMIT" ]; then
            echo "Local repository is not in sync with remote"
            exit 1
        fi
        
        echo "Repository verified successfully"
        """,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Define task dependencies
    check_repo >> [clone_repo, pull_repo] >> verify_sync

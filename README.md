# BI Portal Guidelines
## Overview
**BI Portal** is a robust data pipeline system built with Apache Airflow, a widely-adopted workflow management platform. This system streamlines the process of syncing code from GitHub, managing data in Google BigQuery, and executing SQL queries. Designed to automate and orchestrate essential BI tasks, it simplifies the handling of complex data workflows and analytics.

## Key Features
- **GitHub Synchronization:** Automatically synchronize files between a GitHub repository and the Airflow server, ensuring your code is always up-to-date.
- **BigQuery Dataset and Table Management:** Dynamically create datasets and tables in BigQuery if they don't already exist, streamlining the data management process.
- **Data Ingestion to BigQuery:** Efficiently push data from Pandas DataFrames directly into BigQuery tables, enabling seamless data loading.
- **BigQuery Query Execution:** Run SQL queries on BigQuery, fetch the results, and integrate them into your data workflows for further analysis and reporting.

## Project Structure
```
├── airflow/
│   ├── dags/
│   │   ├── sync_repo.py           # DAG to synchronize files between GitHub and Airflow
│   │   ├── create_dataset.py      # DAG to create BigQuery datasets and tables
│   │   ├── push_data_bigquery.py  # DAG to push DataFrame data to BigQuery
│   │   └── run_bigquery_query.py  # DAG to execute SQL queries on BigQuery
│   └── scripts/
│       ├── data_ingestion.py      # Script for transforming and pushing data
│       ├── dataset_creator.py     # Script for creating datasets and tables
│       └── query_runner.py        # Script for executing queries and handling results
└── README.md
```

## Airflow DAGs
### 1. Sync GitHub Repository:

- Clones or pulls the latest changes from the specified GitHub repository.
- Uses `rsync` to copy the updated files into the Airflow directory.

### 2. Create Dataset and Table in BigQuery:
- Creates a BigQuery dataset if it doesn't already exist.
- Creates a table with the specified schema, or skips creation if the table already exists.

### 3. Push Data to BigQuery:
- Accepts a Pandas DataFrame and pushes it to a BigQuery table.
- Offers configurable options for dataset, table name, and write mode (append, replace, etc.).

### 4. Run BigQuery Query:
- Executes SQL queries on BigQuery.
- Retrieves and processes the results for downstream tasks or reporting.

## Usage
1. **Trigger DAGs:** You can trigger the DAGs manually from the Airflow UI or with CLI commands:
```bash
airflow dags trigger sync_repo
airflow dags trigger create_dataset
airflow dags trigger push_data_bigquery
airflow dags trigger run_bigquery_query
```

2. **Monitor DAG Runs:** Check the status and logs of the DAG runs through the Airflow UI or CLI:
```bash 
airflow dags list-runs --dag-id sync_repo
```

## Configuration
- **GitHub Repository:** Configure the repository URL and local paths in the Airflow DAG.
- **BigQuery Connection:** Set up BigQuery credentials using a service account key and define the connection IDs in Airflow.
- **Dataset and Table Schema:** Define the dataset and table schema directly in the DAG configuration.
- **Schedule:** You can either run the DAGs manually or set up a cron-based schedule.

## Environment Setup
1. **Install Required Packages:**
```bash 
pip install apache-airflow
pip install google-cloud-bigquery pandas
```

2. **Airflow Initialization:**
```bash 
airflow db init
```

3. **Start Airflow Web Server and Scheduler:**
```bash 
airflow webserver --port 8080 &
airflow scheduler &
```

## License
Copyright (C) 2025 [Tung Le Thanh]. All Rights Reserved.

This project is proprietary and confidential. Unauthorized copying, distribution, or modification of this code, via any medium, is strictly prohibited without prior written permission from the author.

# BI Portal Guidelines

## Overview

**BI Portal** is a data pipeline system built with **Apache Airflow** that streamlines the process of syncing code from GitHub, managing data in **BigQuery**, and executing SQL queries. This system is designed to automate and orchestrate essential BI tasks, making it easier to handle data workflows and analytics.

## Features

- **GitHub Sync:** Automatically sync files between a GitHub repository and the Airflow server.
- **Data Ingestion to BigQuery:** Push data from a Pandas DataFrame directly into BigQuery tables.
- **Query Execution on BigQuery:** Run SQL queries on BigQuery, fetch results, and integrate them into your data workflows.

## Project Structure

```
├── airflow/
│   ├── dags/
│   │   ├── sync_repo.py           # DAG to sync files between GitHub and Airflow
│   │   ├── push_data_bigquery.py  # DAG to push DataFrame data to BigQuery
│   │   └── run_bigquery_query.py  # DAG to execute SQL queries on BigQuery
│   └── scripts/
│       ├── data_ingestion.py      # Script for transforming and pushing data
│       └── query_runner.py        # Script for executing queries and handling results
└── README.md
```

## Airflow DAGs

### 1. Sync GitHub Repository

- Clones or pulls the latest changes from the specified GitHub repository.
- Uses `rsync` to copy the updated files into the Airflow directory.

### 2. Push Data to BigQuery

- Accepts a DataFrame and pushes it to a BigQuery table.
- Configurable options for dataset, table name, and write mode (append, replace, etc.).

### 3. Run BigQuery Query

- Executes SQL queries on BigQuery.
- Retrieves and processes results for downstream tasks or reporting.

## Usage

1. **Trigger DAGs:** You can trigger DAGs manually from the Airflow UI or with CLI commands:

```bash
airflow dags trigger sync_repo
airflow dags trigger push_data_bigquery
airflow dags trigger run_bigquery_query
```

2. **Monitor DAG Runs:**
   Check DAG statuses and logs through the Airflow UI or CLI:

```bash
airflow dags list-runs --dag-id sync_repo
```

## Configuration

- **GitHub Repository:** Configure the repository URL and local paths in the Airflow DAG.
- **BigQuery Connection:** Set up BigQuery credentials using a service account key and define connection IDs in Airflow.
- **Schedule:** You can either run DAGs manually or set up a cron-based schedule.

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

---

With this setup, your BI Portal is ready to automate data workflows, keeping your code in sync and making BigQuery interactions seamless! 🚀

Let me know if you’d like me to modify anything or add more details! ✌️


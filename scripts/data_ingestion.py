from google.cloud import bigquery
import os

# Initialize BigQuery client
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

def create_dataset(dataset_id):
    dataset_ref = client.dataset(dataset_id)
    
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Dataset {dataset_id} created.")
    except Exception as e:
        print(f"Dataset {dataset_id} already exists or error occurred: {e}")


def create_table(dataset_id, table_id, schema):
    table_ref = client.dataset(dataset_id).table(table_id)
    
    try:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Table {table_id} created in dataset {dataset_id}.")
    except Exception as e:
        print(f"Table {table_id} already exists or error occurred: {e}")


if __name__ == "__main__":
    dataset_id = "my_dataset"
    table_id = "my_table"
    
    # Define the schema
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    create_dataset(dataset_id)
    create_table(dataset_id, table_id, schema)

    print("Dataset and table setup complete.")

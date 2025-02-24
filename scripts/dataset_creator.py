from google.cloud import bigquery
from google.api_core import exceptions
from typing import List, Optional
import os
import logging

class BigQueryManager:
    def __init__(self, project_id: Optional[str] = None, location: str = "US"):
        """
        Initialize BigQuery manager with project ID and default location.
        
        Args:
            project_id: Optional GCP project ID. If None, uses default from credentials
            location: Geographic location for new datasets (default: "US")
        """
        self.client = bigquery.Client(project=project_id)
        self.location = location
        self.logger = logging.getLogger(__name__)
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def create_dataset(
        self,
        dataset_id: str,
        description: Optional[str] = None,
        labels: Optional[dict] = None
    ) -> bigquery.Dataset:
        """
        Create a new BigQuery dataset if it doesn't exist.
        
        Args:
            dataset_id: ID of the dataset to create
            description: Optional dataset description
            labels: Optional dictionary of labels to apply
            
        Returns:
            The created or existing dataset
            
        Raises:
            Exception: If dataset creation fails for reasons other than already exists
        """
        dataset_ref = self.client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        
        if description:
            dataset.description = description
        if labels:
            dataset.labels = labels

        try:
            return self.client.create_dataset(dataset, timeout=30)
        except exceptions.Conflict:
            self.logger.info(f"Dataset {dataset_id} already exists")
            return self.client.get_dataset(dataset_ref)
        except Exception as e:
            self.logger.error(f"Error creating dataset {dataset_id}: {str(e)}")
            raise

    def create_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: List[bigquery.SchemaField],
        description: Optional[str] = None,
        partition_field: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
        expires_days: Optional[int] = None
    ) -> bigquery.Table:
        """
        Create a new BigQuery table if it doesn't exist.
        
        Args:
            dataset_id: ID of the dataset to create table in
            table_id: ID of the table to create
            schema: List of SchemaField objects defining the table schema
            description: Optional table description
            partition_field: Optional field name for table partitioning
            cluster_fields: Optional list of field names for clustering
            expires_days: Optional number of days until table expiration
            
        Returns:
            The created or existing table
            
        Raises:
            Exception: If table creation fails for reasons other than already exists
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        
        if description:
            table.description = description
            
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
            
        if cluster_fields:
            table.clustering_fields = cluster_fields
            
        if expires_days:
            table.expires = expires_days * 24 * 60 * 60  # Convert days to seconds

        try:
            return self.client.create_table(table)
        except exceptions.Conflict:
            self.logger.info(f"Table {table_id} already exists in dataset {dataset_id}")
            return self.client.get_table(table_ref)
        except Exception as e:
            self.logger.error(f"Error creating table {table_id}: {str(e)}")
            raise

def main():
    # Example configuration
    DATASET_ID = "my_dataset"
    TABLE_ID = "my_table"
    
    # Example schema
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    try:
        # Initialize manager
        bq_manager = BigQueryManager()
        
        # Create dataset with description and labels
        dataset = bq_manager.create_dataset(
            DATASET_ID,
            description="Example dataset",
            labels={"environment": "development"}
        )
        
        # Create table with partitioning and clustering
        table = bq_manager.create_table(
            DATASET_ID,
            TABLE_ID,
            schema,
            description="Example table",
            partition_field="created_at",
            cluster_fields=["name"],
            expires_days=365
        )
        
        print(f"Successfully created/verified dataset {DATASET_ID} and table {TABLE_ID}")
        
    except Exception as e:
        print(f"Setup failed: {str(e)}")

if __name__ == "__main__":
    main()

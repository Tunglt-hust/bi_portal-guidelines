from google.cloud import bigquery
import pandas as pd
from typing import Optional, Union
import logging

class BigQueryLoader:
    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize BigQuery loader with project ID.
        
        Args:
            project_id: Optional GCP project ID. If None, uses default from credentials
        """
        self.client = bigquery.Client(project=project_id)
        self.logger = logging.getLogger(__name__)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        write_mode: str = 'append',
        chunk_size: int = 10000,
        schema: Optional[list] = None,
        create_if_not_exists: bool = True
    ) -> bool:
        """
        Upload a pandas DataFrame to BigQuery.
        
        Args:
            df: Pandas DataFrame to upload
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            write_mode: One of 'append', 'replace', or 'error' (default: 'append')
            chunk_size: Number of rows per chunk when uploading (default: 10000)
            schema: Optional BigQuery schema. If None, will be inferred from DataFrame
            create_if_not_exists: Whether to create the table if it doesn't exist
            
        Returns:
            bool: True if upload was successful, False otherwise
            
        Raises:
            ValueError: If invalid write_mode is specified
        """
        table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
        
        # Validate write mode
        valid_modes = {'append', 'replace', 'error'}
        if write_mode not in valid_modes:
            raise ValueError(f"write_mode must be one of {valid_modes}")
            
        # Convert write_mode to job_config parameters
        write_disposition = {
            'append': 'WRITE_APPEND',
            'replace': 'WRITE_TRUNCATE',
            'error': 'WRITE_EMPTY'
        }[write_mode]

        try:
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=schema,
                # Automatically detect schema if not provided
                autodetect=schema is None
            )
            
            # Check if table exists
            try:
                self.client.get_table(table_ref)
            except Exception:
                if create_if_not_exists:
                    self.logger.info(f"Table {table_ref} does not exist. Creating...")
                else:
                    raise ValueError(f"Table {table_ref} does not exist and create_if_not_exists is False")

            # Upload the dataframe
            self.logger.info(f"Starting upload to {table_ref}")
            
            # Handle empty DataFrame
            if df.empty:
                self.logger.warning("DataFrame is empty. No data will be uploaded.")
                return True
                
            # Upload in chunks
            total_rows = len(df)
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                job = self.client.load_table_from_dataframe(
                    chunk,
                    table_ref,
                    job_config=job_config
                )
                job.result()  # Wait for the job to complete
                
                self.logger.info(f"Uploaded rows {i} to {i + len(chunk)} of {total_rows}")
            
            self.logger.info(f"Successfully uploaded {total_rows} rows to {table_ref}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error uploading to BigQuery: {str(e)}")
            raise

def main():
    # Example usage
    import pandas as pd
    import numpy as np
    
    # Create sample DataFrame
    df = pd.DataFrame({
        'id': range(1000),
        'name': [f'name_{i}' for i in range(1000)],
        'value': np.random.randn(1000),
        'timestamp': pd.date_range(start='2024-01-01', periods=1000)
    })
    
    # Optional: Define schema (if not using autodetect)
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("value", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    try:
        # Initialize loader
        loader = BigQueryLoader()
        
        # Upload DataFrame
        success = loader.upload_dataframe(
            df=df,
            dataset_id='my_dataset',
            table_id='my_table',
            write_mode='append',
            chunk_size=100,
            schema=schema,
            create_if_not_exists=True
        )
        
        if success:
            print("Upload completed successfully")
            
    except Exception as e:
        print(f"Upload failed: {str(e)}")

if __name__ == "__main__":
    main()

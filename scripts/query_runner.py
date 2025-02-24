from google.cloud import bigquery
import os
from typing import List, Dict, Optional

class BigQueryRunner:
    def __init__(self, project_id: str, credentials_path: Optional[str] = None):
        """
        Initialize BigQuery client with project ID and optional credentials path.
        
        Args:
            project_id: GCP project ID
            credentials_path: Path to service account credentials JSON file
        """
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def run_saved_query(self, saved_query_id: str) -> List[Dict]:
        """
        Run a saved query from BigQuery and return results.
        
        Args:
            saved_query_id: ID of the saved query in BigQuery
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            Exception: If query execution fails
        """
        try:
            # Get the saved query
            dataset_ref = self.client.dataset('_saved_queries')
            table_ref = dataset_ref.table(saved_query_id)
            saved_query = self.client.get_table(table_ref)
            
            # Execute the query
            query_job = self.client.query(saved_query.view_query)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            return [dict(row) for row in results]
            
        except Exception as e:
            print(f"Error executing saved query {saved_query_id}: {str(e)}")
            raise

def main():
    # Example configuration
    PROJECT_ID = 'your-project-id'
    CREDENTIALS_PATH = 'path/to/your/credentials.json'  # Optional
    SAVED_QUERY_ID = 'your-saved-query-id'
    
    try:
        # Initialize runner
        runner = BigQueryRunner(PROJECT_ID, CREDENTIALS_PATH)
        
        # Execute query
        results = runner.run_saved_query(SAVED_QUERY_ID)
        
        # Process results
        print(f"Query returned {len(results)} rows:")
        for row in results:
            print(row)
            
    except Exception as e:
        print(f"Failed to execute query: {str(e)}")

if __name__ == '__main__':
    main()

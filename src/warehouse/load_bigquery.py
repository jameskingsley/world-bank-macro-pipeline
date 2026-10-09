import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

load_dotenv()


def get_bigquery_client() -> bigquery.Client:
    """Initializes and returns a BigQuery Client using environment variables."""
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id or project_id == "your-gcp-project-id":
        raise ValueError("GCP_PROJECT_ID environment variable is missing or unconfigured in .env file.")
    return bigquery.Client(project=project_id)


def load_schema(schema_path: str = "config/bigquery_schema.json") -> list:
    """Reads BigQuery JSON schema configuration and parses it into BigQuery SchemaField objects."""
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_json = json.load(f)
    
    return [
        bigquery.SchemaField(
            name=field["name"],
            field_type=field["type"],
            mode=field.get("mode", "NULLABLE"),
            description=field.get("description", "")
        )
        for field in schema_json
    ]


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = "US"):
    """Ensures BigQuery dataset exists, creating it if necessary."""
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"[Warehouse] Dataset '{dataset_id}' exists.")
    except Exception:
        print(f"[Warehouse] Dataset '{dataset_id}' not found. Creating dataset...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset, timeout=30)
        print(f"[Warehouse] Dataset '{dataset_id}' created successfully.")


def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    dataset_id: str = None,
    table_id: str = None,
    write_disposition: str = "WRITE_TRUNCATE"
):
    """
    Loads a validated Pandas DataFrame into a BigQuery table using target schema definitions.
    """
    if df.empty:
        print("[Warehouse] Warning: Input DataFrame is empty. Skipping cloud write.")
        return

    dataset_id = dataset_id or os.getenv("BIGQUERY_DATASET", "world_bank_macro")
    table_id = table_id or os.getenv("BIGQUERY_TABLE", "macro_indicators")
    
    client = get_bigquery_client()
    ensure_dataset_exists(client, dataset_id)
    
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    schema = load_schema()

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.CSV
    )

    print(f"[Warehouse] Initiating load job to BigQuery table: {full_table_id}...")
    
    try:
        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()  # Wait for job completion
        
        table = client.get_table(full_table_id)
        print(f"[Warehouse] Load complete! {table.num_rows} total rows now in {full_table_id}.")
    except GoogleAPIError as exc:
        print(f"[Warehouse] BigQuery Load Error: {exc}")
        raise exc


if __name__ == "__main__":
    from src.ingestion.fetch_worldbank import ingest_macro_data
    from src.validation.validate_duckdb import validate_and_transform_macro_data
    
    raw_df = ingest_macro_data()
    clean_df, _ = validate_and_transform_macro_data(raw_df)
    
    load_dataframe_to_bigquery(clean_df)

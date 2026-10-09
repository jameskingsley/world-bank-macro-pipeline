import argparse
import sys
from pathlib import Path

# Path setup for imports
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.ingestion.fetch_worldbank import ingest_macro_data
from src.validation.validate_duckdb import validate_and_transform_macro_data
from src.warehouse.load_bigquery import load_dataframe_to_bigquery


def run_pipeline(dry_run: bool = False):
    """Executes the World Bank Macroeconomic Ingestion Pipeline."""
    print("=" * 65)
    print("   WORLD BANK MACROECONOMIC INGESTION PIPELINE")
    print("=" * 65)
    
    # Ingestion Stage
    print("\n[STAGE 1/3] Ingesting REST API indicators...")
    raw_df = ingest_macro_data()
    
    if raw_df.empty:
        print("[Pipeline Error] Ingestion returned empty dataset. Aborting run.")
        sys.exit(1)
        
    # Validation Stage (DuckDB)
    print("\n[STAGE 2/3] Executing local schema validation via DuckDB...")
    clean_df, rejected_df = validate_and_transform_macro_data(raw_df)
    
    if clean_df.empty:
        print("[Pipeline Error] No records passed validation. Aborting cloud load.")
        sys.exit(1)
        
    # Warehouse Staging / Cloud Load
    print("\n[STAGE 3/3] Preparing Cloud Warehouse Load...")
    if dry_run:
        print("[Dry Run] Cloud load skipped (--dry-run flag active).")
        print(f"[Dry Run] Summary: {len(clean_df)} records validated and staged for BigQuery.")
    else:
        load_dataframe_to_bigquery(clean_df)
        
    print("\n" + "=" * 65)
    print("   PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Bank Macro Ingestion Pipeline")
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Runs Ingestion and DuckDB Validation without writing to Google BigQuery."
    )
    args = parser.parse_args()
    
    run_pipeline(dry_run=args.dry_run)

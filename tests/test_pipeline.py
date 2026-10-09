import sys
from pathlib import Path
import pandas as pd
import pytest

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.ingestion.fetch_worldbank import load_config, fetch_country_indicator_data, get_retry_session
from src.validation.validate_duckdb import validate_and_transform_macro_data
from src.warehouse.load_bigquery import load_schema


def test_load_config():
    """Verify that YAML indicator configuration loads properly with required keys."""
    config = load_config("config/indicators.yaml")
    assert "indicators" in config
    assert "countries" in config
    assert "start_year" in config
    assert len(config["indicators"]) > 0
    assert "USA" in config["countries"]


def test_fetch_country_indicator_data():
    """Test API extraction for a single indicator and country returns structured records."""
    session = get_retry_session()
    indicator = {"code": "NY.GDP.MKTP.CD", "name": "GDP (Current US$)", "category": "Economic Growth"}
    records = fetch_country_indicator_data(
        session=session,
        indicator=indicator,
        country_code="USA",
        start_year=2020,
        end_year=2022
    )
    assert isinstance(records, list)
    assert len(records) > 0
    first_record = records[0]
    assert first_record["country_code"] == "USA"
    assert first_record["indicator_code"] == "NY.GDP.MKTP.CD"
    assert "value" in first_record


def test_duckdb_validation_and_transformations():
    """Verify DuckDB handles schema casting, unit scaling, and YoY window calculations."""
    mock_raw_data = pd.DataFrame([
        {
            "country_code": "USA",
            "country_name": "United States",
            "indicator_code": "NY.GDP.MKTP.CD",
            "indicator_name": "GDP (Current US$)",
            "category": "Economic Growth",
            "year": 2021,
            "value": 23000000000000.0,
            "ingested_at": "2026-10-09T00:00:00"
        },
        {
            "country_code": "USA",
            "country_name": "United States",
            "indicator_code": "NY.GDP.MKTP.CD",
            "indicator_name": "GDP (Current US$)",
            "category": "Economic Growth",
            "year": 2022,
            "value": 25000000000000.0,
            "ingested_at": "2026-10-09T00:00:00"
        },
        # Record with missing value to trigger rejection branch
        {
            "country_code": "NGA",
            "country_name": "Nigeria",
            "indicator_code": "FP.CPI.TOTL.ZG",
            "indicator_name": "Inflation",
            "category": "Prices",
            "year": 2022,
            "value": None,
            "ingested_at": "2026-10-09T00:00:00"
        }
    ])

    clean_df, rejected_df = validate_and_transform_macro_data(mock_raw_data)

    # Check passed and rejected record counts
    assert len(clean_df) == 2
    assert len(rejected_df) == 1
    assert rejected_df.iloc[0]["rejection_reason"] == "Null Indicator Value"

    # Verify unit formatting (GDP in Billions)
    gdp_2022 = clean_df[clean_df["year"] == 2022].iloc[0]
    assert gdp_2022["value_formatted"] == 25000.0
    assert gdp_2022["unit_of_measure"] == "USD Billions"

    # Verify YoY calculation (25000B - 23000B raw = 2e12 absolute change)
    assert gdp_2022["yoy_absolute_change"] == 2000000000000.0


def test_bigquery_schema_loading():
    """Ensure BigQuery JSON schema loads expected field definitions."""
    schema = load_schema("config/bigquery_schema.json")
    field_names = [field.name for field in schema]
    assert "country_code" in field_names
    assert "indicator_code" in field_names
    assert "value_formatted" in field_names
    assert "yoy_absolute_change" in field_names

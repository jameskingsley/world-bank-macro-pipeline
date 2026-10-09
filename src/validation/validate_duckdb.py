import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import duckdb
import pandas as pd
from typing import Tuple


def validate_and_transform_macro_data(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Uses DuckDB to execute SQL validation, transformations, YoY growth calculation,
    and unit normalization prior to BigQuery staging.
    """
    if df_raw.empty:
        print("[Transformation] Warning: Input DataFrame is empty.")
        return pd.DataFrame(), pd.DataFrame()

    con = duckdb.connect(database=":memory:")
    con.register("raw_macro_stage", df_raw)
    
    # SQL Transformation & Feature Engineering Pipeline
    transformation_query = """
        WITH cleaned_base AS (
            SELECT 
                UPPER(TRIM(country_code)) AS country_code,
                TRIM(country_name) AS country_name,
                UPPER(TRIM(indicator_code)) AS indicator_code,
                TRIM(indicator_name) AS indicator_name,
                TRIM(category) AS category,
                CAST(year AS INT) AS year,
                CAST(value AS DOUBLE) AS raw_value,
                CAST(ingested_at AS TIMESTAMP) AS ingested_at
            FROM raw_macro_stage
            WHERE country_code IS NOT NULL
              AND LENGTH(TRIM(country_code)) = 3
              AND indicator_code IS NOT NULL
              AND year IS NOT NULL
              AND year BETWEEN 1950 AND 2100
              AND value IS NOT NULL
        ),
        transformed_features AS (
            SELECT 
                country_code,
                country_name,
                indicator_code,
                indicator_name,
                category,
                year,
                raw_value AS value,
                -- Unit Normalization: Scale raw currency indicators into Billions
                CASE 
                    WHEN indicator_code = 'NY.GDP.MKTP.CD' THEN ROUND(raw_value / 1e9, 2)
                    ELSE ROUND(raw_value, 2)
                END AS value_formatted,
                CASE 
                    WHEN indicator_code = 'NY.GDP.MKTP.CD' THEN 'USD Billions'
                    WHEN indicator_code IN ('FP.CPI.TOTL.ZG', 'SL.UEM.TOTL.ZS', 'GC.DOD.TOTL.GD.ZS', 'NE.EXP.GNFS.ZS') THEN 'Percentage (%)'
                    ELSE 'Raw Units'
                END AS unit_of_measure,
                -- Window function for Year-over-Year (YoY) Change Rate
                ROUND(
                    raw_value - LAG(raw_value) OVER (
                        PARTITION BY country_code, indicator_code 
                        ORDER BY year ASC
                    ), 2
                ) AS yoy_absolute_change,
                CAST(ingested_at AS TIMESTAMP) AS ingested_at
            FROM cleaned_base
        )
        SELECT * FROM transformed_features
        ORDER BY country_code, indicator_code, year DESC
    """
    
    rejected_query = """
        SELECT 
            *,
            CASE 
                WHEN country_code IS NULL OR LENGTH(TRIM(country_code)) != 3 THEN 'Invalid ISO-3 Country Code'
                WHEN indicator_code IS NULL THEN 'Missing Indicator Code'
                WHEN year IS NULL OR year NOT BETWEEN 1950 AND 2100 THEN 'Invalid Year Range'
                WHEN value IS NULL THEN 'Null Indicator Value'
                ELSE 'Unknown Issue'
            END AS rejection_reason
        FROM raw_macro_stage
        WHERE country_code IS NULL 
           OR LENGTH(TRIM(country_code)) != 3
           OR indicator_code IS NULL
           OR year IS NULL
           OR year NOT BETWEEN 1950 AND 2100
           OR value IS NULL
    """
    
    clean_df = con.execute(transformation_query).df()
    rejected_df = con.execute(rejected_query).df()
    
    con.close()
    
    print(f"[Transformation] DuckDB Transformations & Features Applied:")
    print(f"  - Valid & Transformed Records: {len(clean_df)}")
    print(f"  - Rejected Records (Nulls/Missing): {len(rejected_df)}")
    
    return clean_df, rejected_df


if __name__ == "__main__":
    from src.ingestion.fetch_worldbank import ingest_macro_data
    
    raw_df = ingest_macro_data()
    clean_df, rejected_df = validate_and_transform_macro_data(raw_df)
    
    print("\n--- Transformed Sample Data ---")
    print(clean_df[["country_code", "indicator_code", "year", "value_formatted", "unit_of_measure", "yoy_absolute_change"]].head(10))

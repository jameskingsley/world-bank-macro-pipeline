import datetime
import time
import requests
import yaml
import pandas as pd
from typing import Dict, List, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def load_config(config_path: str = "config/indicators.yaml") -> Dict[str, Any]:
    """Loads YAML configuration file for indicators and countries."""
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_retry_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """Creates a Requests session configured with exponential backoff retries."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_country_indicator_data(
    session: requests.Session,
    indicator: Dict[str, str],
    country_code: str,
    start_year: int,
    end_year: int
) -> List[Dict[str, Any]]:
    """Fetches macro indicator data for a single country with error handling."""
    indicator_code = indicator["code"]
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}"
    
    params = {
        "format": "json",
        "date": f"{start_year}:{end_year}",
        "per_page": 1000
    }
    
    try:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        print(f"  [Warning] Failed request for {country_code} - {indicator_code}: {exc}")
        return []
    
    # World Bank API returns a list where payload[1] contains data points
    if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
        return []

    records = []
    ingestion_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    for row in payload[1]:
        records.append({
            "country_code": row.get("countryiso3code") or country_code,
            "country_name": row["country"]["value"] if row.get("country") else None,
            "indicator_code": indicator_code,
            "indicator_name": indicator.get("name"),
            "category": indicator.get("category"),
            "year": int(row["date"]) if row.get("date") else None,
            "value": float(row["value"]) if row.get("value") is not None else None,
            "ingested_at": ingestion_time
        })
        
    return records


def ingest_macro_data(config_path: str = "config/indicators.yaml") -> pd.DataFrame:
    """Orchestrates API calls across configured indicators and countries into a unified DataFrame."""
    config = load_config(config_path)
    session = get_retry_session()
    all_records = []
    
    for ind in config["indicators"]:
        print(f"[Ingestion] Fetching {ind['code']} ({ind['name']})...")
        for country in config["countries"]:
            records = fetch_country_indicator_data(
                session=session,
                indicator=ind,
                country_code=country,
                start_year=config["start_year"],
                end_year=config["end_year"]
            )
            all_records.extend(records)
            time.sleep(0.1)  # Gentle rate-limiting throttle
            
    df = pd.DataFrame(all_records)
    print(f"[Ingestion] Extraction complete. Total records fetched: {len(df)}")
    return df


if __name__ == "__main__":
    df_raw = ingest_macro_data()
    print("\n--- Sample Extracted Data ---")
    print(df_raw.head())

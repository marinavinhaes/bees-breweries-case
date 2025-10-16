# fetcher.py
import requests
import json
import os
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_BASE = "https://api.openbrewerydb.org/v1/breweries"  # full list endpoint (supports pagination)

def fetch_all_breweries(per_page: int = 50, max_pages: int = 100) -> List[Dict]:
    """
    Fetch all breweries by paginating the API until no more results.
    """
    results = []
    for page in range(1, max_pages + 1):
        params = {"page":page, "per_page":per_page}

        #Python library to send an HTTP GET request to the API endpoint defined by API_BASE
        resp = requests.get(API_BASE, params=params, timeout=30)
        # API_BASE = https://api.openbrewerydb.org/breweries?page=2&per_page=50

        if resp.status_code != 200: 
            logger.error( f"Failed fetching page {page}: {resp.status_code} {resp.text}")
            resp.raise_for_status()

        page_data = resp.json()
        if not page_data:
            logger.info(f"No more data after page {page-1}, stopping pagination.")
            break

        #extend = takes all the elements of page_data and adds them individually into results
        results.extend(page_data)
        logger.info(f"Fetched page {page}, items={len(page_data)}")


    else:
        # only runs if loop didn't break early
        logger.warning("Reached max_pages; stopping pagination")
    
    return results

def write_bronze(data: List[Dict], bronze_dir: str) -> str:
    """
    Persist raw data (JSON) to bronze layer as a single file with timestamp.
    Returns the path written.
    """
    os.makedirs(bronze_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(bronze_dir, f"openbrewery_raw_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logger.info(f"Wrote bronze file: {out_path}")
    return out_path

def run_fetch_and_persist(bronze_dir: str):
    data = fetch_all_breweries()
    return write_bronze(data, bronze_dir)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bronze-dir", default="/data/lake/bronze")
    args = p.parse_args()
    run_fetch_and_persist(args.bronze_dir)
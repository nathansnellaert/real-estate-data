"""Fetch housing market data from Redfin Data Center.

Downloads TSV files from Redfin's S3 bucket containing housing market data
at various geographic levels (national, state, metro, county, city, zip, neighborhood).
"""

from subsets_utils import get, save_raw_file, load_state, save_state

BASE_URL = "https://redfin-public-data.s3.us-west-2.amazonaws.com"

# Available datasets with their S3 paths
DATASETS = {
    # Weekly data (most recent)
    "weekly_housing_market": {
        "path": "redfin_covid19/weekly_housing_market_data_most_recent.tsv000.gz",
        "description": "Weekly housing market data (most recent)",
    },
    # Monthly market tracker by geography
    "market_tracker_national": {
        "path": "redfin_market_tracker/us_national_market_tracker.tsv000.gz",
        "description": "National-level monthly market data",
    },
    "market_tracker_metro": {
        "path": "redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz",
        "description": "Metro-level monthly market data",
    },
    "market_tracker_state": {
        "path": "redfin_market_tracker/state_market_tracker.tsv000.gz",
        "description": "State-level monthly market data",
    },
    "market_tracker_county": {
        "path": "redfin_market_tracker/county_market_tracker.tsv000.gz",
        "description": "County-level monthly market data",
    },
    "market_tracker_city": {
        "path": "redfin_market_tracker/city_market_tracker.tsv000.gz",
        "description": "City-level monthly market data",
    },
    "market_tracker_zip": {
        "path": "redfin_market_tracker/zip_code_market_tracker.tsv000.gz",
        "description": "ZIP code-level monthly market data",
    },
    "market_tracker_neighborhood": {
        "path": "redfin_market_tracker/neighborhood_market_tracker.tsv000.gz",
        "description": "Neighborhood-level monthly market data",
    },
}


def run():
    """Fetch Redfin housing market data for all geography levels."""
    state = load_state("redfin_ingest")
    completed = set(state.get("completed", []))

    # Build list of downloads needed
    downloads = [(key, config) for key, config in DATASETS.items() if key not in completed]

    if not downloads:
        print("  All datasets up to date")
        return

    print(f"  Fetching {len(downloads)} files...")

    for i, (dataset_id, config) in enumerate(downloads, 1):
        url = f"{BASE_URL}/{config['path']}"
        print(f"  [{i}/{len(downloads)}] {dataset_id}...")
        print(f"    {config['description']}")

        response = get(url, timeout=300.0)
        response.raise_for_status()

        # Save the gzipped TSV file directly (bytes are handled automatically)
        save_raw_file(response.content, dataset_id, extension="tsv.gz")

        completed.add(dataset_id)
        save_state("redfin_ingest", {"completed": list(completed)})

        print(f"    -> saved ({len(response.content):,} bytes)")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()

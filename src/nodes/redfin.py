"""Fetch housing market data from Redfin Data Center.

Downloads monthly market tracker TSV files from Redfin's S3 bucket at
national, state, metro, and county levels.

Skipped (documented decision):
- Weekly data: overlaps with monthly, less stable
- City/ZIP/neighborhood: very large files, low marginal value over county
"""

from subsets_utils import get, save_raw_file, load_state, save_state

BASE_URL = "https://redfin-public-data.s3.us-west-2.amazonaws.com"

# Monthly market tracker by geography
DATASETS = {
    "market_tracker_national": {
        "path": "redfin_market_tracker/us_national_market_tracker.tsv000.gz",
        "description": "National-level monthly market data",
    },
    "market_tracker_state": {
        "path": "redfin_market_tracker/state_market_tracker.tsv000.gz",
        "description": "State-level monthly market data",
    },
    "market_tracker_metro": {
        "path": "redfin_market_tracker/redfin_metro_market_tracker.tsv000.gz",
        "description": "Metro-level monthly market data",
    },
    "market_tracker_county": {
        "path": "redfin_market_tracker/county_market_tracker.tsv000.gz",
        "description": "County-level monthly market data",
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

"""Fetch Realtor.com metrics from S3."""

from subsets_utils import get, save_raw_file

# Core Metrics - inventory, prices, days on market
CORE_DATASETS = {
    "core_state": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_State_History.csv",
    "core_metro": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_Metro_History.csv",
    "core_county": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv",
}

# Hotness Metrics - market competitiveness indicators
HOTNESS_DATASETS = {
    "hotness_metro": "https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_Metro_History.csv",
}


def run():
    """Fetch all Realtor.com datasets."""
    all_datasets = {**CORE_DATASETS, **HOTNESS_DATASETS}

    for name, url in all_datasets.items():
        print(f"  Fetching {name}...")
        response = get(url, timeout=300.0)
        response.raise_for_status()
        save_raw_file(response.text, name, extension="csv")
        print(f"    Saved ({len(response.text) / 1024 / 1024:.1f} MB)")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()

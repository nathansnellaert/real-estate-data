"""Transform Zillow Observed Rent Index (ZORI) data by region type."""

import pyarrow as pa
from subsets_utils import merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from ._common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, standardize_columns, to_table
)
from .zillow_download import run as download_zillow

def make_metadata(region_type: str) -> dict:
    """Generate metadata for a rent dataset."""
    label = REGION_TYPE_LABELS[region_type]
    col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "rent": "Typical monthly rent in USD",
    }
    if region_type != "state":
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"zillow_rent_{region_type}",
        "title": f"Zillow Observed Rent Index by {label}",
        "description": f"Zillow Observed Rent Index (ZORI) by {label.lower()}. ZORI is a smoothed, seasonally adjusted measure of the typical observed market rate rent. Includes single-family, condo, and multifamily rentals.",
        "license": "Zillow - free for non-commercial use with attribution",
        "column_descriptions": col_descs
    }

def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_rent_{region_type} output. Raises AssertionError on failure."""
    min_rows = {
        "metro": 5000,
        "state": 500,
        "county": 10000,
        "city": 50000,
        "zip": 100000,
    }.get(region_type, 500)

    # Build columns spec - state_code may be null type for state-level data
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
        "rent": "double",
    }
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"

    validate(table, {
        "columns": columns_spec,
        "not_null": ["date", "region_id", "region_name", "rent"],
        "unique": ["date", "region_id"],
        "min_rows": min_rows,
    })

    assert_valid_date(table, "date")
    assert_positive(table, "rent", allow_zero=False)

    # Check rent ranges
    rents = [v for v in table.column("rent").to_pylist() if v is not None]
    assert min(rents) >= 100, f"Rents seem too low: min={min(rents)}"
    assert max(rents) <= 200000, f"Rents seem too high: max={max(rents)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"ZORI data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"

def run():
    """Transform rent data for all region types."""
    for region_type in REGION_TYPES:
        print(f"\n  Processing rent {region_type}...")

        raw_name = f"zori_{region_type}"
        df = load_and_melt(raw_name, "rent")

        if df.empty:
            print(f"    No data found for {region_type}, skipping")
            continue

        df = standardize_columns(df, region_type, ["rent"])

        if df.empty:
            print(f"    No data after filtering for {region_type}, skipping")
            continue

        table = to_table(df)
        print(f"    {region_type}: {table.num_rows:,} rows")

        dataset_id = f"zillow_rent_{region_type}"
        test(table, region_type)
        # Key includes all non-metric columns; rent is the only metric
        key_cols = [c for c in table.column_names if c != "rent"]
        merge(table, dataset_id, key=key_cols)
        publish(dataset_id, make_metadata(region_type))

NODES = {
    run: [download_zillow],
}

if __name__ == "__main__":
    run()

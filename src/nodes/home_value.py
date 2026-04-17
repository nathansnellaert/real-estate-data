"""Transform Zillow Home Value Index (ZHVI) data by region type."""

import pyarrow as pa
from subsets_utils import merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from ._common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, merge_variants, standardize_columns, to_table
)
from .zillow_download import run as download_zillow

ZHVI_VARIANTS = {
    "zhvi_all_homes": "all_homes",
    "zhvi_sfr": "single_family",
    "zhvi_condo": "condo",
    "zhvi_1bed": "bed_1",
    "zhvi_2bed": "bed_2",
    "zhvi_3bed": "bed_3",
    "zhvi_4bed": "bed_4",
    "zhvi_5bed_plus": "bed_5_plus",
    "zhvi_bottom_tier": "bottom_tier",
    "zhvi_top_tier": "top_tier",
}

def make_metadata(region_type: str) -> dict:
    """Generate metadata for a home value dataset."""
    label = REGION_TYPE_LABELS[region_type]
    col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "all_homes": "Typical home value for all homes (SFR + Condo), mid-tier (35th-65th percentile)",
        "single_family": "Typical home value for single-family residences, mid-tier",
        "condo": "Typical home value for condos/co-ops, mid-tier",
        "bed_1": "Typical home value for 1-bedroom homes",
        "bed_2": "Typical home value for 2-bedroom homes",
        "bed_3": "Typical home value for 3-bedroom homes",
        "bed_4": "Typical home value for 4-bedroom homes",
        "bed_5_plus": "Typical home value for 5+ bedroom homes",
        "bottom_tier": "Typical home value for bottom-tier (5th-35th percentile)",
        "top_tier": "Typical home value for top-tier (65th-95th percentile)",
    }
    if region_type != "state":
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"zillow_home_value_{region_type}",
        "title": f"Zillow Home Value Index by {label}",
        "description": f"Zillow Home Value Index (ZHVI) by {label.lower()}. ZHVI is a smoothed, seasonally adjusted measure of the typical home value. Includes different property types (all homes, single-family, condo), bedroom counts (1-5+), and price tiers (bottom, mid, top).",
        "license": "Zillow - free for non-commercial use with attribution",
        "column_descriptions": col_descs
    }

def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_home_value_{region_type} output. Raises AssertionError on failure."""
    value_columns = [
        "all_homes", "single_family", "condo",
        "bed_1", "bed_2", "bed_3", "bed_4", "bed_5_plus",
        "bottom_tier", "top_tier"
    ]

    # Build columns spec dynamically based on what's in the table
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
    }
    # state_code may be null type for state-level data (no parent state)
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"
    for col in value_columns:
        if col in table.column_names:
            columns_spec[col] = "double"

    min_rows = {
        "metro": 10000,
        "state": 1000,
        "county": 50000,
        "city": 100000,
        "zip": 500000,
    }.get(region_type, 1000)

    validate(table, {
        "columns": columns_spec,
        "not_null": ["date", "region_id", "region_name"],
        "unique": ["date", "region_id"],
        "min_rows": min_rows,
    })

    assert_valid_date(table, "date")

    # Check value ranges for present columns
    for col in value_columns:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 50_000_000, f"{col} values seem too high: max={max(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 1990, f"Data goes back too far: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"

def run():
    """Transform home value data for all region types."""
    value_cols = list(ZHVI_VARIANTS.values())

    for region_type in REGION_TYPES:
        print(f"\n  Processing {region_type}...")

        dfs = []
        for raw_prefix, col_name in ZHVI_VARIANTS.items():
            raw_name = f"{raw_prefix}_{region_type}"
            df = load_and_melt(raw_name, col_name)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            print(f"    No data found for {region_type}, skipping")
            continue

        merged = merge_variants(dfs, value_cols)
        merged = standardize_columns(merged, region_type, value_cols)

        if merged.empty:
            print(f"    No data after filtering for {region_type}, skipping")
            continue

        table = to_table(merged)
        print(f"    {region_type}: {table.num_rows:,} rows")

        dataset_id = f"zillow_home_value_{region_type}"
        test(table, region_type)
        # Key includes all non-metric columns
        metric_cols = {"all_homes", "single_family", "condo", "bed_1", "bed_2", "bed_3", "bed_4", "bed_5_plus", "bottom_tier", "top_tier"}
        key_cols = [c for c in table.column_names if c not in metric_cols]
        merge(table, dataset_id, key=key_cols)
        publish(dataset_id, make_metadata(region_type))

NODES = {
    run: [download_zillow],
}

if __name__ == "__main__":
    run()

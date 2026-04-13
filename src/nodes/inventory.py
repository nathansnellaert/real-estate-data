"""Transform Zillow Inventory data by region type."""

import pyarrow as pa
from subsets_utils import merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from ._common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, merge_variants, standardize_columns, to_table
)
from .zillow_download import run as download_zillow

INVENTORY_VARIANTS = {
    "inventory_for_sale": "for_sale_inventory",
    "new_listings": "new_listings",
    "new_pending": "new_pending",
}

def make_metadata(region_type: str) -> dict:
    """Generate metadata for an inventory dataset."""
    label = REGION_TYPE_LABELS[region_type]
    col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "for_sale_inventory": "Number of for-sale listings active during the month",
        "new_listings": "Number of new listings during the month",
        "new_pending": "Number of listings that went pending during the month",
    }
    if region_type != "state":
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"zillow_inventory_{region_type}",
        "title": f"Zillow Housing Inventory by {label}",
        "description": f"Zillow housing inventory metrics by {label.lower()}. Includes for-sale inventory count, new listings count, and new pending sales count. Property type is single-family residences and condos.",
        "column_descriptions": col_descs
    }

def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_inventory_{region_type} output. Raises AssertionError on failure."""
    value_columns = ["for_sale_inventory", "new_listings", "new_pending"]

    # Build columns spec dynamically based on what's in the table
    columns_spec = {
        "date": "string",
        "region_id": "int",
        "region_name": "string",
    }
    # state_code may be null type for state-level data
    if "state_code" in table.column_names:
        state_col_type = str(table.schema.field("state_code").type)
        if state_col_type != "null":
            columns_spec["state_code"] = "string"
    for col in value_columns:
        if col in table.column_names:
            columns_spec[col] = "double"

    min_rows = {
        "metro": 5000,
        "state": 500,
        "county": 20000,
        "city": 50000,
        "zip": 100000,
    }.get(region_type, 500)

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
                assert max(values) <= 10_000_000, f"{col} values seem too high: max={max(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"Inventory data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"

def run():
    """Transform inventory data for all region types."""
    value_cols = list(INVENTORY_VARIANTS.values())

    for region_type in REGION_TYPES:
        print(f"\n  Processing inventory {region_type}...")

        dfs = []
        for raw_prefix, col_name in INVENTORY_VARIANTS.items():
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

        dataset_id = f"zillow_inventory_{region_type}"
        test(table, region_type)
        # Key includes all non-metric columns
        metric_cols = {"for_sale_inventory", "new_listings", "new_pending"}
        key_cols = [c for c in table.column_names if c not in metric_cols]
        merge(table, dataset_id, key=key_cols)
        publish(dataset_id, make_metadata(region_type))

NODES = {
    run: [download_zillow],
}

if __name__ == "__main__":
    run()

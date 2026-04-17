"""Transform Zillow Sales data by region type."""

import pyarrow as pa
from subsets_utils import merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from ._common import (
    REGION_TYPES, REGION_TYPE_LABELS,
    load_and_melt, merge_variants, standardize_columns, to_table
)
from .zillow_download import run as download_zillow

SALES_VARIANTS = {
    "median_list_price": "median_list_price",
    "median_sale_price": "median_sale_price",
    "sales_count": "sales_count",
    "pct_sold_above_list": "pct_sold_above_list",
    "pct_sold_below_list": "pct_sold_below_list",
    "days_to_pending": "days_to_pending",
    "price_cut_share": "pct_price_cut",
}

def make_metadata(region_type: str, columns: list) -> dict:
    """Generate metadata for a sales dataset."""
    label = REGION_TYPE_LABELS[region_type]
    all_col_descs = {
        "date": "End of month date (YYYY-MM-DD)",
        "region_id": "Zillow region identifier",
        "region_name": f"{label} name",
        "state_code": "Two-letter US state code",
        "median_list_price": "Median list price in USD",
        "median_sale_price": "Median sale price in USD",
        "sales_count": "Estimated number of sales (nowcast)",
        "pct_sold_above_list": "Percent of sales above final list price",
        "pct_sold_below_list": "Percent of sales below final list price",
        "days_to_pending": "Mean days from listing to pending",
        "pct_price_cut": "Percent of listings with a price cut",
    }
    col_descs = {k: v for k, v in all_col_descs.items() if k in columns}

    return {
        "id": f"zillow_sales_{region_type}",
        "title": f"Zillow Sales Metrics by {label}",
        "description": f"Zillow sales and pricing metrics by {label.lower()}. Includes median list/sale prices, sales counts, days to pending, and price cut statistics. Property type is single-family residences and condos.",
        "license": "Zillow - free for non-commercial use with attribution",
        "column_descriptions": col_descs
    }

def test(table: pa.Table, region_type: str) -> None:
    """Validate zillow_sales_{region_type} output. Raises AssertionError on failure."""
    value_columns = [
        "median_list_price", "median_sale_price", "sales_count",
        "pct_sold_above_list", "pct_sold_below_list",
        "days_to_pending", "pct_price_cut"
    ]

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

    # Check price ranges
    for col in ["median_list_price", "median_sale_price"]:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 100_000_000, f"{col} values seem too high: max={max(values)}"

    # Check percentage ranges
    for col in ["pct_sold_above_list", "pct_sold_below_list", "pct_price_cut"]:
        if col in table.column_names:
            values = [v for v in table.column(col).to_pylist() if v is not None]
            if values:
                assert min(values) >= 0, f"{col} has negative values: min={min(values)}"
                assert max(values) <= 100, f"{col} values exceed 100%: max={max(values)}"

    # Check days range
    if "days_to_pending" in table.column_names:
        values = [v for v in table.column("days_to_pending").to_pylist() if v is not None]
        if values:
            assert min(values) >= 0, f"days_to_pending has negative values: min={min(values)}"
            assert max(values) <= 365, f"days_to_pending seems too high: max={max(values)}"

    # Check sales_count
    if "sales_count" in table.column_names:
        values = [v for v in table.column("sales_count").to_pylist() if v is not None]
        if values:
            assert min(values) >= 0, f"sales_count has negative values: min={min(values)}"

    # Check date range
    dates = table.column("date").to_pylist()
    min_year = int(min(dates)[:4])
    max_year = int(max(dates)[:4])
    assert min_year >= 2010, f"Sales data shouldn't go back before 2010: {min_year}"
    assert max_year <= 2030, f"Data goes into the future: {max_year}"

def run():
    """Transform sales data for all region types."""
    value_cols = list(SALES_VARIANTS.values())

    for region_type in REGION_TYPES:
        print(f"\n  Processing sales {region_type}...")

        dfs = []
        for raw_prefix, col_name in SALES_VARIANTS.items():
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

        dataset_id = f"zillow_sales_{region_type}"
        test(table, region_type)
        merge(table, dataset_id, key=["date", "region_id"])
        publish(dataset_id, make_metadata(region_type, table.column_names))

NODES = {
    run: [download_zillow],
}

if __name__ == "__main__":
    run()

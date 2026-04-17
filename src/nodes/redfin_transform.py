"""Transform Redfin housing market tracker data by geography level.

Redfin provides monthly market data at national, state, metro, county, city,
zip, and neighborhood levels. We transform the monthly market tracker data
(not the weekly data, which overlaps and is less stable).

We skip the weekly dataset and neighborhood level to keep scope manageable.
City and zip are also skipped due to size (hundreds of millions of rows).
"""

import gzip
import pandas as pd
import pyarrow as pa
from io import BytesIO
from subsets_utils import load_raw_file, merge, publish, validate
from subsets_utils.testing import assert_valid_date
from .redfin import run as download_redfin


# Columns to keep from the market tracker (drop MOM/YOY momentum columns)
VALUE_COLS = [
    "MEDIAN_SALE_PRICE",
    "MEDIAN_LIST_PRICE",
    "MEDIAN_PPSF",
    "MEDIAN_LIST_PPSF",
    "HOMES_SOLD",
    "PENDING_SALES",
    "NEW_LISTINGS",
    "INVENTORY",
    "MONTHS_OF_SUPPLY",
    "MEDIAN_DOM",
    "AVG_SALE_TO_LIST",
    "SOLD_ABOVE_LIST",
    "PRICE_DROPS",
    "OFF_MARKET_IN_TWO_WEEKS",
]

# Map raw column names to clean snake_case
COLUMN_RENAME = {
    "PERIOD_BEGIN": "date",
    "REGION_TYPE": "region_type",
    "REGION": "region_name",
    "STATE_CODE": "state_code",
    "PROPERTY_TYPE": "property_type",
    "MEDIAN_SALE_PRICE": "median_sale_price",
    "MEDIAN_LIST_PRICE": "median_list_price",
    "MEDIAN_PPSF": "median_price_per_sqft",
    "MEDIAN_LIST_PPSF": "median_list_price_per_sqft",
    "HOMES_SOLD": "homes_sold",
    "PENDING_SALES": "pending_sales",
    "NEW_LISTINGS": "new_listings",
    "INVENTORY": "inventory",
    "MONTHS_OF_SUPPLY": "months_of_supply",
    "MEDIAN_DOM": "median_days_on_market",
    "AVG_SALE_TO_LIST": "avg_sale_to_list_ratio",
    "SOLD_ABOVE_LIST": "pct_sold_above_list",
    "PRICE_DROPS": "pct_price_drops",
    "OFF_MARKET_IN_TWO_WEEKS": "pct_off_market_two_weeks",
}

# Datasets to transform: raw_name -> (dataset_id suffix, min_rows)
GEO_LEVELS = {
    "market_tracker_national": ("national", 100),
    "market_tracker_state": ("state", 5_000),
    "market_tracker_metro": ("metro", 50_000),
    "market_tracker_county": ("county", 50_000),
}


def _load_redfin_tsv(raw_name: str) -> pd.DataFrame:
    """Load a gzipped TSV from raw storage."""
    raw_bytes = load_raw_file(raw_name, extension="tsv.gz", binary=True)
    decompressed = gzip.decompress(raw_bytes)
    df = pd.read_csv(BytesIO(decompressed), sep="\t", dtype={"STATE_CODE": str})
    return df


def _transform_tracker(raw_name: str, geo_level: str) -> pa.Table:
    """Transform a market tracker TSV into a clean table."""
    df = _load_redfin_tsv(raw_name)

    # Filter to seasonally adjusted "All Residential" data only
    if "IS_SEASONALLY_ADJUSTED" in df.columns:
        df = df[df["IS_SEASONALLY_ADJUSTED"] == True]
    if "PROPERTY_TYPE" in df.columns:
        df = df[df["PROPERTY_TYPE"] == "All Residential"]

    # Keep only the columns we need
    keep_raw = ["PERIOD_BEGIN", "REGION_TYPE", "REGION", "STATE_CODE"] + VALUE_COLS
    keep_raw = [c for c in keep_raw if c in df.columns]
    df = df[keep_raw].copy()

    # Rename columns
    df = df.rename(columns=COLUMN_RENAME)

    # Clean date
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Clean region_name (strip whitespace and quotes)
    if "region_name" in df.columns:
        df["region_name"] = df["region_name"].str.strip().str.strip('"')

    # Drop region_type column (redundant since each file is one level)
    if "region_type" in df.columns:
        df = df.drop(columns=["region_type"])

    # For national level, drop state_code (meaningless)
    if geo_level == "national" and "state_code" in df.columns:
        df = df.drop(columns=["state_code"])

    # Convert value columns to numeric
    value_cols_clean = [COLUMN_RENAME.get(c, c) for c in VALUE_COLS if COLUMN_RENAME.get(c, c) in df.columns]
    for col in value_cols_clean:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all value columns are null
    if value_cols_clean:
        df = df.dropna(subset=value_cols_clean, how="all")

    df = df.sort_values(["date", "region_name"]).reset_index(drop=True)

    return pa.Table.from_pandas(df, preserve_index=False)


def _make_metadata(geo_level: str) -> dict:
    """Build metadata for a Redfin market tracker dataset."""
    level_label = {
        "national": "National",
        "state": "State",
        "metro": "Metro Area",
        "county": "County",
    }[geo_level]

    col_descs = {
        "date": "First of month date (YYYY-MM-DD)",
        "region_name": f"{level_label} name",
        "median_sale_price": "Median sale price in USD",
        "median_list_price": "Median list price in USD",
        "median_price_per_sqft": "Median sale price per square foot in USD",
        "median_list_price_per_sqft": "Median list price per square foot in USD",
        "homes_sold": "Number of homes sold",
        "pending_sales": "Number of pending sales",
        "new_listings": "Number of new listings",
        "inventory": "Active inventory count",
        "months_of_supply": "Months of housing supply (inventory / sales rate)",
        "median_days_on_market": "Median days on market",
        "avg_sale_to_list_ratio": "Average sale-to-list price ratio (1.0 = sold at list)",
        "pct_sold_above_list": "Share of homes sold above list price (0-1)",
        "pct_price_drops": "Share of listings with price drops (0-1)",
        "pct_off_market_two_weeks": "Share of homes going off market within two weeks (0-1)",
    }
    if geo_level not in ("national",):
        col_descs["state_code"] = "Two-letter US state code"

    return {
        "id": f"redfin_market_{geo_level}",
        "title": f"Redfin Housing Market by {level_label}",
        "description": f"Redfin monthly housing market data at the {level_label.lower()} level. Seasonally adjusted, all residential property types. Includes sale prices, inventory, days on market, and market competitiveness metrics.",
        "license": "Redfin - free for non-commercial use with attribution",
        "column_descriptions": col_descs,
    }


def run():
    """Transform Redfin market tracker data for all geography levels."""
    for raw_name, (geo_level, min_rows) in GEO_LEVELS.items():
        print(f"\n  Processing Redfin {geo_level}...")

        table = _transform_tracker(raw_name, geo_level)
        print(f"    {geo_level}: {table.num_rows:,} rows")

        dataset_id = f"redfin_market_{geo_level}"

        # Determine key based on geo level
        if geo_level == "national":
            key = ["date"]
        else:
            key = ["date", "region_name"]

        validate(table, {
            "not_null": ["date", "region_name"] if geo_level != "national" else ["date"],
            "unique": key,
            "min_rows": min_rows,
        })
        assert_valid_date(table, "date")

        merge(table, dataset_id, key=key)
        publish(dataset_id, _make_metadata(geo_level))


NODES = {
    run: [download_redfin],
}

if __name__ == "__main__":
    run()

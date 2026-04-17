"""Transform Realtor.com housing market data.

Core metrics: inventory, prices, days on market by state/metro/county.
Hotness metrics: market competitiveness scores by metro.
"""

import pandas as pd
import pyarrow as pa
from io import StringIO
from subsets_utils import load_raw_file, merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from .realtor import run as download_realtor


# Columns to keep from the core datasets (drop _mm/_yy momentum columns)
CORE_KEEP_COLS = [
    "month_date_yyyymm",
    "median_listing_price",
    "active_listing_count",
    "median_days_on_market",
    "new_listing_count",
    "price_reduced_count",
    "price_reduced_share",
    "pending_listing_count",
    "median_listing_price_per_square_foot",
    "median_square_feet",
    "average_listing_price",
    "total_listing_count",
    "pending_ratio",
]

# Region-specific ID columns
REGION_ID_COLS = {
    "core_state": ["state", "state_id"],
    "core_metro": ["cbsa_code", "cbsa_title"],
    "core_county": ["county_fips", "county_name"],
}


def _load_core(raw_name: str, region_cols: list) -> pd.DataFrame:
    """Load and clean a Realtor.com core metrics CSV."""
    csv_text = load_raw_file(raw_name, extension="csv")
    df = pd.read_csv(StringIO(csv_text))

    # Keep only the columns we need
    keep = region_cols + [c for c in CORE_KEEP_COLS if c in df.columns]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Build date from yyyymm
    df["date"] = pd.to_datetime(df["month_date_yyyymm"].astype(str), format="%Y%m")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["month_date_yyyymm"])

    # Convert numeric columns
    numeric_cols = [c for c in df.columns if c not in region_cols + ["date"]]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop quality_flag if present
    if "quality_flag" in df.columns:
        df = df.drop(columns=["quality_flag"])

    return df


def _make_core_metadata(geo_level: str, region_desc: str, id_col_descs: dict) -> dict:
    """Build metadata for a core metrics dataset."""
    col_descs = {
        "date": "First of month date (YYYY-MM-DD)",
        **id_col_descs,
        "median_listing_price": "Median listing price in USD",
        "active_listing_count": "Number of active listings",
        "median_days_on_market": "Median days on market",
        "new_listing_count": "Number of new listings",
        "price_reduced_count": "Number of listings with price reduction",
        "price_reduced_share": "Share of listings with price reduction (0-1)",
        "pending_listing_count": "Number of pending listings",
        "median_listing_price_per_square_foot": "Median listing price per square foot in USD",
        "median_square_feet": "Median listing square footage",
        "average_listing_price": "Average listing price in USD",
        "total_listing_count": "Total number of listings (active + pending)",
        "pending_ratio": "Ratio of pending to total listings (0-1)",
    }
    return {
        "id": f"realtor_market_{geo_level}",
        "title": f"Realtor.com Housing Market by {region_desc}",
        "description": f"Housing market metrics from Realtor.com by {region_desc.lower()}. Includes listing prices, inventory counts, days on market, price reductions, and pending sales.",
        "license": "Realtor.com - free for public use with attribution",
        "column_descriptions": col_descs,
    }


CORE_CONFIGS = {
    "core_state": {
        "geo_level": "state",
        "region_desc": "State",
        "region_cols": ["state", "state_id"],
        "rename": {"state": "state_name", "state_id": "state_code"},
        "id_col_descs": {
            "state_name": "Full state name",
            "state_code": "Two-letter state code",
        },
        "key": ["date", "state_code"],
        "min_rows": 5_000,
    },
    "core_metro": {
        "geo_level": "metro",
        "region_desc": "Metro Area (CBSA)",
        "region_cols": ["cbsa_code", "cbsa_title"],
        "rename": {"cbsa_code": "cbsa_code", "cbsa_title": "metro_name"},
        "id_col_descs": {
            "cbsa_code": "Core Based Statistical Area (CBSA) code",
            "metro_name": "Metro area name",
        },
        "key": ["date", "cbsa_code"],
        "min_rows": 20_000,
    },
    "core_county": {
        "geo_level": "county",
        "region_desc": "County",
        "region_cols": ["county_fips", "county_name"],
        "rename": {"county_fips": "county_fips", "county_name": "county_name"},
        "id_col_descs": {
            "county_fips": "County FIPS code",
            "county_name": "County name",
        },
        "key": ["date", "county_fips"],
        "min_rows": 50_000,
    },
}


def transform_core():
    """Transform Realtor.com core metrics for all geography levels."""
    for raw_name, config in CORE_CONFIGS.items():
        print(f"\n  Processing Realtor.com {config['geo_level']}...")

        df = _load_core(raw_name, config["region_cols"])
        df = df.rename(columns=config["rename"])

        # Sort
        key = config["key"]
        df = df.sort_values(key).reset_index(drop=True)

        table = pa.Table.from_pandas(df, preserve_index=False)
        print(f"    {config['geo_level']}: {table.num_rows:,} rows")

        dataset_id = f"realtor_market_{config['geo_level']}"

        # Validate
        validate(table, {
            "not_null": ["date"] + config["key"],
            "unique": config["key"],
            "min_rows": config["min_rows"],
        })
        assert_valid_date(table, "date")

        metadata = _make_core_metadata(
            config["geo_level"], config["region_desc"], config["id_col_descs"]
        )
        merge(table, dataset_id, key=config["key"])
        publish(dataset_id, metadata)


# --- Hotness Metrics ---

HOTNESS_METADATA = {
    "id": "realtor_hotness_metro",
    "title": "Realtor.com Market Hotness by Metro Area",
    "description": "Realtor.com Market Hotness Index by metro area (CBSA). Combines supply and demand scores into an overall hotness score ranking the most competitive housing markets.",
    "license": "Realtor.com - free for public use with attribution",
    "column_descriptions": {
        "date": "First of month date (YYYY-MM-DD)",
        "cbsa_code": "Core Based Statistical Area (CBSA) code",
        "metro_name": "Metro area name",
        "hotness_rank": "Overall market hotness rank (1 = hottest)",
        "hotness_score": "Overall market hotness score (0-100)",
        "supply_score": "Supply-side score (0-100, higher = tighter supply)",
        "demand_score": "Demand-side score (0-100, higher = more demand)",
        "median_days_on_market": "Median days on market",
        "median_listing_price": "Median listing price in USD",
    },
}


def transform_hotness():
    """Transform Realtor.com hotness metrics for metros."""
    print("\n  Processing Realtor.com hotness (metro)...")

    csv_text = load_raw_file("hotness_metro", extension="csv")
    df = pd.read_csv(StringIO(csv_text))

    # Build date
    df["date"] = pd.to_datetime(df["month_date_yyyymm"].astype(str), format="%Y%m")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Keep only the columns we need
    df = df.rename(columns={
        "cbsa_code": "cbsa_code",
        "cbsa_title": "metro_name",
    })

    keep_cols = [
        "date", "cbsa_code", "metro_name",
        "hotness_rank", "hotness_score", "supply_score", "demand_score",
        "median_days_on_market", "median_listing_price",
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    # Convert numeric columns
    for col in ["hotness_rank", "hotness_score", "supply_score", "demand_score",
                "median_days_on_market", "median_listing_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["date", "cbsa_code"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    print(f"    hotness: {table.num_rows:,} rows")

    validate(table, {
        "not_null": ["date", "cbsa_code", "metro_name"],
        "unique": ["date", "cbsa_code"],
        "min_rows": 10_000,
    })
    assert_valid_date(table, "date")

    merge(table, "realtor_hotness_metro", key=["date", "cbsa_code"])
    publish("realtor_hotness_metro", HOTNESS_METADATA)


NODES = {
    transform_core: [download_realtor],
    transform_hotness: [download_realtor],
}

if __name__ == "__main__":
    transform_core()
    transform_hotness()

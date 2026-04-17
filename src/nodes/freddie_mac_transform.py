"""Transform Freddie Mac data: FMHPI (House Price Index) and PMMS (Mortgage Rates)."""

import pandas as pd
import pyarrow as pa
from io import StringIO
from subsets_utils import load_raw_file, merge, publish, validate
from subsets_utils.testing import assert_valid_date, assert_positive
from .freddie_mac import run as download_freddie_mac


# --- FMHPI (House Price Index) ---

FMHPI_METADATA = {
    "id": "freddie_mac_house_price_index",
    "title": "Freddie Mac House Price Index",
    "description": "Freddie Mac House Price Index (FMHPI) measuring changes in US single-family home prices at national, state, and metro (CBSA) levels. Includes both seasonally adjusted and non-seasonally adjusted indices. Base period is January 2000 = 100.",
    "license": "Freddie Mac - free for public use with attribution",
    "column_descriptions": {
        "date": "First of month date (YYYY-MM-DD)",
        "geo_type": "Geographic level: USA, State, or CBSA (metro)",
        "geo_name": "Geographic area name",
        "geo_code": "Geographic area code (FIPS state code or CBSA code)",
        "index_nsa": "House price index, not seasonally adjusted (Jan 2000 = 100)",
        "index_sa": "House price index, seasonally adjusted (Jan 2000 = 100)",
    },
}


def transform_fmhpi():
    """Transform Freddie Mac House Price Index data."""
    csv_text = load_raw_file("freddie_mac_hpi", extension="csv")
    df = pd.read_csv(StringIO(csv_text))

    # Build date from Year + Month
    df["date"] = pd.to_datetime(
        df["Year"].astype(str) + "-" + df["Month"].astype(str).str.zfill(2) + "-01"
    ).dt.strftime("%Y-%m-%d")

    # Clean geo_code: Freddie Mac uses "." for missing
    df["GEO_Code"] = df["GEO_Code"].replace(".", "").astype(str)

    df = df.rename(columns={
        "GEO_Type": "geo_type",
        "GEO_Name": "geo_name",
        "GEO_Code": "geo_code",
        "Index_NSA": "index_nsa",
        "Index_SA": "index_sa",
    })

    df = df[["date", "geo_type", "geo_name", "geo_code", "index_nsa", "index_sa"]]

    # Strip whitespace from string columns
    for col in ["geo_type", "geo_name", "geo_code"]:
        df[col] = df[col].str.strip()

    df = df.sort_values(["date", "geo_type", "geo_name"]).reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    print(f"    FMHPI: {table.num_rows:,} rows")

    # Validate
    validate(table, {
        "columns": {
            "date": "string",
            "geo_type": "string",
            "geo_name": "string",
            "geo_code": "string",
            "index_nsa": "double",
            "index_sa": "double",
        },
        "not_null": ["date", "geo_type", "geo_name", "index_nsa", "index_sa"],
        "unique": ["date", "geo_type", "geo_name"],
        "min_rows": 100_000,
    })
    assert_valid_date(table, "date")
    assert_positive(table, "index_nsa", allow_zero=False)
    assert_positive(table, "index_sa", allow_zero=False)

    merge(table, "freddie_mac_house_price_index", key=["date", "geo_type", "geo_name"])
    publish("freddie_mac_house_price_index", FMHPI_METADATA)


# --- PMMS (Mortgage Rates) ---

PMMS_METADATA = {
    "id": "freddie_mac_mortgage_rates",
    "title": "Freddie Mac Primary Mortgage Market Survey",
    "description": "Weekly mortgage rates from Freddie Mac's Primary Mortgage Market Survey (PMMS), the longest running weekly survey of mortgage rates in the United States. Covers 30-year fixed, 15-year fixed, and 5/1 adjustable-rate mortgages.",
    "license": "Freddie Mac - free for public use with attribution",
    "column_descriptions": {
        "date": "Survey date (YYYY-MM-DD, weekly on Thursdays)",
        "rate_30yr_fixed": "Average 30-year fixed-rate mortgage rate (%)",
        "points_30yr_fixed": "Average points for 30-year fixed-rate mortgage",
        "rate_15yr_fixed": "Average 15-year fixed-rate mortgage rate (%)",
        "points_15yr_fixed": "Average points for 15-year fixed-rate mortgage",
        "rate_5yr_arm": "Average 5/1 adjustable-rate mortgage rate (%)",
        "margin_5yr_arm": "Average margin for 5/1 adjustable-rate mortgage",
        "spread_5yr_arm": "Spread between 5/1 ARM and 30-year fixed (%)",
    },
}


def transform_pmms():
    """Transform Freddie Mac Primary Mortgage Market Survey data."""
    csv_text = load_raw_file("freddie_mac_pmms", extension="csv")
    df = pd.read_csv(StringIO(csv_text))

    # Parse date
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    df = df.rename(columns={
        "pmms30": "rate_30yr_fixed",
        "pmms30p": "points_30yr_fixed",
        "pmms15": "rate_15yr_fixed",
        "pmms15p": "points_15yr_fixed",
        "pmms51": "rate_5yr_arm",
        "pmms51m": "margin_5yr_arm",
        "pmms51spread": "spread_5yr_arm",
    })

    # Convert to numeric (some entries may be blank)
    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("date").reset_index(drop=True)

    table = pa.Table.from_pandas(df, preserve_index=False)
    print(f"    PMMS: {table.num_rows:,} rows")

    validate(table, {
        "columns": {
            "date": "string",
            "rate_30yr_fixed": "double",
            "points_30yr_fixed": "double",
            "rate_15yr_fixed": "double",
            "points_15yr_fixed": "double",
            "rate_5yr_arm": "double",
            "margin_5yr_arm": "double",
            "spread_5yr_arm": "double",
        },
        "not_null": ["date", "rate_30yr_fixed"],
        "unique": ["date"],
        "min_rows": 2000,
    })
    assert_valid_date(table, "date")

    # Sanity check rate ranges
    rates = [v for v in table.column("rate_30yr_fixed").to_pylist() if v is not None]
    assert min(rates) >= 1.0, f"30yr rate too low: {min(rates)}"
    assert max(rates) <= 25.0, f"30yr rate too high: {max(rates)}"

    merge(table, "freddie_mac_mortgage_rates", key=["date"])
    publish("freddie_mac_mortgage_rates", PMMS_METADATA)


NODES = {
    transform_fmhpi: [download_freddie_mac],
    transform_pmms: [download_freddie_mac],
}

if __name__ == "__main__":
    transform_fmhpi()
    transform_pmms()

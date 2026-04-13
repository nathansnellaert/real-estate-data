"""Common utilities for Zillow transforms."""

import pandas as pd
import pyarrow as pa
from io import StringIO
from subsets_utils import load_raw_file


REGION_TYPES = ["metro", "state", "county", "city", "zip"]

REGION_TYPE_FILTER = {
    "metro": "msa",
    "state": "state",
    "county": "county",
    "city": "city",
    "zip": "zip",
}

REGION_TYPE_LABELS = {
    "metro": "Metro Area (MSA)",
    "state": "State",
    "county": "County",
    "city": "City",
    "zip": "ZIP Code",
}


def load_and_melt(raw_name: str, value_col: str) -> pd.DataFrame:
    """Load a Zillow CSV and melt it to long format."""
    try:
        csv_text = load_raw_file(raw_name, extension="csv")
    except FileNotFoundError:
        return pd.DataFrame()

    df = pd.read_csv(StringIO(csv_text), dtype={"RegionName": str})

    id_vars = [c for c in df.columns if not c[0].isdigit()]
    df = df.melt(id_vars=id_vars, var_name="date", value_name=value_col)
    return df


def merge_variants(dfs: list, value_cols: list) -> pd.DataFrame:
    """Merge multiple melted dataframes on RegionID + date."""
    if not dfs:
        return pd.DataFrame()

    merged = dfs[0]
    for df in dfs[1:]:
        value_col = [c for c in df.columns if c in value_cols][0]
        df_subset = df[["RegionID", "date", value_col]].copy()
        merged = merged.merge(df_subset, on=["RegionID", "date"], how="outer")

    return merged


def standardize_columns(df: pd.DataFrame, region_type: str, value_cols: list) -> pd.DataFrame:
    """Rename columns, filter region type, handle state_code, format date."""
    # Filter to the correct region type
    region_filter = REGION_TYPE_FILTER[region_type]
    if "RegionType" in df.columns:
        df = df[df["RegionType"].str.lower() == region_filter]

    if df.empty:
        return df

    # Rename columns
    df = df.rename(columns={
        "RegionID": "region_id",
        "RegionName": "region_name",
        "StateName": "state_code",
    })

    # Drop unnecessary columns
    drop_cols = [c for c in ["SizeRank", "RegionType"] if c in df.columns]
    df = df.drop(columns=drop_cols)

    # Handle state_code
    if "state_code" in df.columns:
        if region_type == "state":
            df = df.drop(columns=["state_code"])
        else:
            df["state_code"] = df["state_code"].fillna("").astype(str)

    # Convert date format
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # Convert value columns to numeric
    for col in value_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all value columns are null
    existing_value_cols = [c for c in value_cols if c in df.columns]
    if existing_value_cols:
        df = df.dropna(subset=existing_value_cols, how="all")

    # Sort
    df = df.sort_values(["date", "region_name"]).reset_index(drop=True)

    return df


def to_table(df: pd.DataFrame) -> pa.Table:
    """Convert DataFrame to PyArrow Table."""
    return pa.Table.from_pandas(df, preserve_index=False)

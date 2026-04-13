"""Fetch raw CSV data from Zillow Research for all region types and metrics."""

from subsets_utils import get, save_raw_file, load_state, save_state

BASE_URL = "https://files.zillowstatic.com/research/public_csvs"

# Region types available in Zillow data
REGION_TYPES = ["Metro", "State", "County", "City", "Zip"]

# Dataset configurations: (metric_folder, filename_pattern, description)
# filename_pattern uses {region} placeholder for region type
DATASETS = {
    # Home Value Index (ZHVI) - typical home value
    "zhvi_all_homes": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI All Homes (SFR + Condo) - Mid Tier",
    },
    "zhvi_sfr": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_uc_sfr_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI Single Family Residence - Mid Tier",
    },
    "zhvi_condo": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI Condo/Co-op - Mid Tier",
    },
    "zhvi_1bed": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_bdrmcnt_1_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI 1 Bedroom",
    },
    "zhvi_2bed": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_bdrmcnt_2_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI 2 Bedroom",
    },
    "zhvi_3bed": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_bdrmcnt_3_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI 3 Bedroom",
    },
    "zhvi_4bed": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_bdrmcnt_4_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI 4 Bedroom",
    },
    "zhvi_5bed_plus": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_bdrmcnt_5_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
        "description": "ZHVI 5+ Bedroom",
    },
    "zhvi_bottom_tier": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_uc_sfrcondo_tier_0.0_0.33_sm_sa_month.csv",
        "description": "ZHVI Bottom Tier (5th-35th percentile)",
    },
    "zhvi_top_tier": {
        "folder": "zhvi",
        "pattern": "{region}_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_month.csv",
        "description": "ZHVI Top Tier (65th-95th percentile)",
    },
    # Rent Index (ZORI)
    "zori": {
        "folder": "zori",
        "pattern": "{region}_zori_uc_sfrcondomfr_sm_sa_month.csv",
        "description": "ZORI Observed Rent Index",
    },
    # Inventory
    "inventory_for_sale": {
        "folder": "invt_fs",
        "pattern": "{region}_invt_fs_uc_sfrcondo_sm_month.csv",
        "description": "For-Sale Inventory",
    },
    "new_listings": {
        "folder": "new_listings",
        "pattern": "{region}_new_listings_uc_sfrcondo_sm_month.csv",
        "description": "New Listings Count",
    },
    "new_pending": {
        "folder": "new_pending",
        "pattern": "{region}_new_pending_uc_sfrcondo_sm_month.csv",
        "description": "New Pending Sales",
    },
    # Prices
    "median_list_price": {
        "folder": "mlp",
        "pattern": "{region}_mlp_uc_sfrcondo_sm_month.csv",
        "description": "Median List Price",
    },
    "median_sale_price": {
        "folder": "median_sale_price",
        "pattern": "{region}_median_sale_price_uc_sfrcondo_sm_sa_month.csv",
        "description": "Median Sale Price",
    },
    # Sales metrics
    "sales_count": {
        "folder": "sales_count_now",
        "pattern": "{region}_sales_count_now_uc_sfrcondo_month.csv",
        "description": "Sales Count Nowcast",
    },
    "pct_sold_above_list": {
        "folder": "pct_sold_above_list",
        "pattern": "{region}_pct_sold_above_list_uc_sfrcondo_sm_month.csv",
        "description": "Percent Sold Above List Price",
    },
    "pct_sold_below_list": {
        "folder": "pct_sold_below_list",
        "pattern": "{region}_pct_sold_below_list_uc_sfrcondo_sm_month.csv",
        "description": "Percent Sold Below List Price",
    },
    "days_to_pending": {
        "folder": "mean_doz_pending",
        "pattern": "{region}_mean_doz_pending_uc_sfrcondo_sm_month.csv",
        "description": "Mean Days to Pending",
    },
    # Price cuts
    "price_cut_share": {
        "folder": "perc_listings_price_cut",
        "pattern": "{region}_perc_listings_price_cut_uc_sfrcondo_sm_month.csv",
        "description": "Percent of Listings with Price Cut",
    },
}


def run():
    """Fetch Zillow research CSV datasets for all region types."""
    state = load_state("zillow_ingest")
    completed = set(state.get("completed", []))

    # Build list of all downloads needed
    downloads = []
    for dataset_id, config in DATASETS.items():
        for region in REGION_TYPES:
            key = f"{dataset_id}_{region.lower()}"
            if key not in completed:
                downloads.append((dataset_id, region, config, key))

    if not downloads:
        print("  All datasets up to date")
        return

    print(f"  Fetching {len(downloads)} files...")

    for i, (dataset_id, region, config, key) in enumerate(downloads, 1):
        filename = config["pattern"].format(region=region)
        url = f"{BASE_URL}/{config['folder']}/{filename}"

        print(f"  [{i}/{len(downloads)}] {dataset_id} ({region})...")

        try:
            response = get(url, timeout=120.0)
            response.raise_for_status()

            # Save with region type in filename
            save_raw_file(response.text, f"{dataset_id}_{region.lower()}", extension="csv")

            completed.add(key)
            save_state("zillow_ingest", {"completed": list(completed)})

            print(f"    -> saved ({len(response.text):,} bytes)")
        except Exception as e:
            print(f"    -> FAILED: {e}")
            # Continue to next file, don't abort entire run


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()

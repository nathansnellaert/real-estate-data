"""Fetch Freddie Mac housing data.

- FMHPI: House Price Index
- PMMS: Primary Mortgage Market Survey (mortgage rates)
"""

from subsets_utils import get, save_raw_file

FMHPI_URL = "https://www.freddiemac.com/fmac-resources/research/docs/fmhpi_master_file.csv"
PMMS_URL = "https://www.freddiemac.com/pmms/docs/PMMS_history.csv"


def run():
    """Fetch Freddie Mac data files."""
    print("  Fetching FMHPI (House Price Index)...")
    response = get(FMHPI_URL, timeout=300)
    response.raise_for_status()
    save_raw_file(response.text, "freddie_mac_hpi", extension="csv")
    print(f"    Saved freddie_mac_hpi.csv ({len(response.text):,} bytes)")

    print("  Fetching PMMS (Mortgage Rates)...")
    response = get(PMMS_URL, timeout=300)
    response.raise_for_status()
    save_raw_file(response.text, "freddie_mac_pmms", extension="csv")
    print(f"    Saved freddie_mac_pmms.csv ({len(response.text):,} bytes)")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()

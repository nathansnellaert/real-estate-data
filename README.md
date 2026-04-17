# real-estate-data

US housing market data from four sources:

## Sources

| Source | Datasets | Coverage |
|--------|----------|----------|
| **Zillow** | Home values (ZHVI), rent (ZORI), inventory, sales metrics | Metro, state, county, city, ZIP |
| **Freddie Mac** | House Price Index (FMHPI), mortgage rates (PMMS) | National, state, CBSA |
| **Realtor.com** | Core market metrics, market hotness | State, metro, county |
| **Redfin** | Monthly market tracker | National, state, metro, county |

## Scope decisions

- **Zillow**: All available metrics and region types. Bedroom breakdowns (1-5+) and price tiers (bottom/mid/top) included for ZHVI.
- **Freddie Mac**: Both FMHPI (house price index) and PMMS (weekly mortgage rates). Full history back to 1975 (HPI) and 1971 (PMMS).
- **Realtor.com**: Core metrics (prices, inventory, DOM) at state/metro/county + hotness scores at metro level. Month-over-month and year-over-year momentum columns dropped (derivable from the level values).
- **Redfin**: Monthly market tracker at national/state/metro/county. Seasonally adjusted, "All Residential" property type only. Weekly data, city/zip/neighborhood levels skipped to keep scope manageable.

## Output datasets

**Zillow** (20 datasets): `zillow_home_value_{region}`, `zillow_rent_{region}`, `zillow_inventory_{region}`, `zillow_sales_{region}` for each of metro/state/county/city/zip.

**Freddie Mac** (2 datasets): `freddie_mac_house_price_index`, `freddie_mac_mortgage_rates`.

**Realtor.com** (4 datasets): `realtor_market_state`, `realtor_market_metro`, `realtor_market_county`, `realtor_hotness_metro`.

**Redfin** (4 datasets): `redfin_market_national`, `redfin_market_state`, `redfin_market_metro`, `redfin_market_county`.

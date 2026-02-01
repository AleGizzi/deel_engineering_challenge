# Part 1: Analytics Engineering Challenge

## 1. Preliminary Data Exploration

### Data Sources

Two CSV files here - both have exactly 5,430 rows.

**acceptance_report.csv** - This file contains the transaction details:
- 11 columns total
- Transactions start from 2019-01-01 onwards
- Key fields observations: `external_ref` (possible unique ID), `date_time` in ISO format, `state` is either ACCEPTED or DECLINED
- Amounts are in different currencies (USD, EUR, GBP, CAD, MXN, etc.)
- There's a `rates` column that's JSON - contains exchange rates for converting to USD
- `cvv_provided` is TRUE/FALSE as a string (gotta convert that)

**chargeback_report.csv** - Contains only 4 columns:
- Same `external_ref` field (key join for both tables)
- `chargeback` flag is TRUE/FALSE as a string
- Same 5,430 rows, so should match up with acceptance report

### Initial Observations

First observations:

1. The data types are all strings - even booleans are "TRUE"/"FALSE" strings. Will need to set data types.
2. The `rates` column is JSON stored as text. I'll use DuckDB so it will need to be parsed properly.
3. Dates are ISO-8601 format - consistent.
4. The `external_ref` looks like it should be unique and joinable between the two files. Confirmed/
5. Multiple currencies means we'll need to convert everything to USD for apples-to-apples comparison.

No obvious nulls in critical fields, and the state values are only ACCEPTED/DECLINED (no weird typos). The JSON in rates looks valid too.

### Quick Insights

From scanning through the data:
- Multiple countries  (US, UK, France, Mexico, Canada, UAE, etc.)
- Mix of currencies - USD is the settlement currency according to the API spec
- Not all transactions are accepted 
- Chargebacks seem to be a subset of accepted transactions
- CVV isn't always provided 

## 2. Model Architecture

I've followed a dbt layered approach - staging → intermediate → marts. 

### The Flow

```
CSV files (seeds)
    ↓
staging models (clean & standardize)
    ↓  
intermediate (join & business logic)
    ↓
marts (aggregated for analytics)
```

### Staging Layer

I put these in `models/staging/globepay/` following the dbt best practices guide. The naming follows `stg_globepay__[entity]` pattern.

**stg_globepay__acceptance_report** does the initial clean up:
- Converts those string booleans to actual booleans
- Parses the JSON rates column
- Converts amounts to USD using the exchange rates
- Extracts date parts (year, month, day of week) for easier filtering later
- Casts everything to proper types

**stg_globepay__chargeback_report** flags and standardization:
- Just cleans up the chargeback flag
- Standardizes the column names

Both are materialized as views since they're lightweight transformations.

### Intermediate Layer

**int_payment_transactions** flags and metrics calculations:
- Left joins chargeback data to acceptance data (using external_ref)
- Creates boolean flags for is_accepted, is_declined
- Calculates revenue metrics - gross (all accepted) and net (accepted minus chargebacks)
- Added an `is_missing_chargeback_data` flag to track when chargeback records don't exist

I used a LEFT JOIN here because we want to keep all transactions even if chargeback data is missing. The flag helps distinguish "no chargeback" from "missing data".

### Marts Layer

These are materialized as tables for performance since they're aggregated:

**mart_daily_metrics** - Daily rollups:
- Transaction counts by day
- Acceptance/decline rates
- Daily revenue totals
- Good for time-series analysis

**mart_country_metrics** - Country-level aggregations:
- Totals by country/currency
- Includes declined transaction amounts (needed for Q2)
- Average transaction sizes
- CVV acceptance rates
- Date ranges

**mart_payment_summary** - More granular daily breakdown:
- By date, country, AND currency
- Useful for deeper dives

### Why This Structure?

I went with views for staging/intermediate because:
- They're always fresh (recompute on each run)
- Less storage overhead
- Fast enough for this dataset size

Tables for marts because:
- Pre-aggregated = faster queries
- Analysts will query these frequently
- Worth the storage cost

If this dataset grows significantly, we'd probably want to make marts incremental, but for now this works fine.

## 3. Lineage Graphs

dbt generates these automatically with

```bash
dbt docs generate
dbt docs serve
```

Then open http://localhost:8080 

The flow looks like:
- acceptance_report seed → stg_globepay__acceptance_report → int_payment_transactions → marts
- chargeback_report seed → stg_globepay__chargeback_report → int_payment_transactions → marts

Everything converges at `int_payment_transactions` which then feeds the three mart models. Clean, no circular dependencies.



## 4. Macros, Validation, and Documentation

### Macros

Honestly, I didn't create any custom macros for this project. The logic wasn't repeated enough to justify it. But if I were to add some, here's what would make sense:

**Currency conversion macro** - The USD conversion logic appears in the staging model. If we had more models doing currency conversion, a macro would help.

**Percentage calculation macro** - We calculate acceptance rates, chargeback rates, etc. in multiple places. A macro could standardize the formula and null handling.

**Date truncation** - If we needed weekly/monthly aggregations, a macro for consistent date grouping would be useful.



### Data Validation

I set up tests in the YAML files:

**Schema tests** (in `_globepay__models.yml` and `schema.yml`):
- `external_ref` is unique and not null
- `state` only has ACCEPTED/DECLINED values
- `acceptance_rate_pct` is between 0-100
- Transaction dates are not null

**Other possible tests**:
- A test to ensure chargebacks only happen on accepted transactions
- A test that net_revenue <= gross_revenue
- Maybe some tests on the exchange rate parsing


### Documentation

I documented models in YAML files (`_globepay__models.yml`, `schema.yml`). Each model has:
- A description of what it does
- Column descriptions
- Tests defined inline

The sources are documented in `_globepay__sources.yml` - describes the raw CSV structure.



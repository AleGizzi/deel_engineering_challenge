# Part 2: Production Model for Data Analyst


## Question 1: What is the acceptance rate over time?

This one's pretty straightforward. I built `mart_daily_metrics` which rolls up transactions by day and calculates the acceptance rate.

**The model:** `globepay_analytics/models/marts/mart_daily_metrics.sql`

**How to query it:**
```sql
SELECT
    transaction_date,
    acceptance_rate_pct,
    accepted_transactions,
    total_transactions
FROM mart_daily_metrics
ORDER BY transaction_date;
```

The `acceptance_rate_pct` field is calculated as `(accepted_transactions / total_transactions) * 100` for each day. 

**Answer Summary:**
- **Date Range**: 2019-01-01 to 2019-06-30 (181 days)
- **Average Acceptance Rate**: 69.56%
- **Minimum Acceptance Rate**: 43.33%
- **Maximum Acceptance Rate**: 90.00%

**Key Insights:**
- Acceptance rates vary significantly day-to-day, ranging from 43.33% to 90.00%
- The average acceptance rate of 69.56% suggests that approximately 7 out of 10 transactions are accepted
- Daily transaction volume appears consistent at 30 transactions per day


## Question 2: List countries where declined transactions went over $25M

For this, I added `total_declined_amount_usd` to the `mart_country_metrics` model. It sums up all the declined transaction amounts by country and currency.

**The model:** `globepay_analytics/models/marts/mart_country_metrics.sql`

**The query:**
```sql
SELECT
    country,
    currency,
    total_declined_amount_usd,
    declined_transactions,
    total_transactions
FROM mart_country_metrics
WHERE total_declined_amount_usd > 25000000
ORDER BY total_declined_amount_usd DESC;
```


The $25M threshold is hardcoded in the WHERE clause -  could make it a parameter if needed.

**Answer:** 4 countries/currency combinations exceeded $25M in declined transactions:

| Country | Currency | Declined Amount ($M) | Declined Count | Total Transactions | Decline Rate % |
|---------|----------|---------------------|----------------|-------------------|----------------|
| FR      | EUR      | 32.63               | 271            | 905               | 29.94          |
| UK      | GBP      | 27.49               | 258            | 905               | 28.51          |
| AE      | USD      | 26.34               | 291            | 905               | 32.15          |
| US      | USD      | 25.13               | 297            | 905               | 32.82          |

**Key Insights:**
- **France (EUR)** has the highest declined amount at $32.63M
- **United States (USD)** has the highest decline rate at 32.82%
- All four countries have similar total transaction volumes (905 transactions each)
- The decline rates range from 28.51% to 32.82%, indicating significant transaction rejection rates in these markets



## Question 3: Which transactions are missing chargeback data?

This was a bit trickier. The issue is that we need to distinguish between:
- Transactions that have a chargeback record saying "no chargeback" (chargeback = FALSE)
- Transactions that don't have a chargeback record at all

I added an `is_missing_chargeback_data` flag to `int_payment_transactions` that's TRUE when there's no chargeback record for a transaction.

**The model:** `globepay_analytics/models/intermediate/int_payment_transactions.sql`

**The query:**
```sql
SELECT
    external_ref,
    transaction_timestamp,
    transaction_date,
    country,
    currency,
    state,
    amount_usd,
    is_missing_chargeback_data
FROM int_payment_transactions
WHERE is_missing_chargeback_data = true
ORDER BY transaction_date DESC, amount_usd DESC;
```

The flag is set by checking if `chargeback.external_ref` is NULL after the LEFT JOIN. If it's NULL, that means the transaction exists in acceptance but not in chargeback.

**Why this matters:**
If chargeback data is missing, you might want to investigate why. Could be a data pipeline issue, or maybe chargebacks are only tracked for certain transaction types. The flag makes it easy to identify these cases.
**Answer:** **0 transactions are missing chargeback data**

**Summary:**
- All 5,430 transactions in the acceptance report have corresponding records in the chargeback report
- The data is complete - there are no gaps in chargeback tracking
- This indicates good data quality and complete coverage of chargeback information

**Note:** The `is_missing_chargeback_data` flag was added to identify any data quality issues, but in this dataset, all transactions have chargeback records (even if the chargeback value is FALSE).


## Model Dependencies

Here's how everything connects:

```
acceptance_report (seed)
    └──> stg_globepay__acceptance_report
            └──> int_payment_transactions ──> mart_daily_metrics (Q1)
                                              └──> mart_country_metrics (Q2)

chargeback_report (seed)
    └──> stg_globepay__chargeback_report
            └──> int_payment_transactions (Q3)
```


## Notes for Analysts

- All amounts are in USD (converted from original currency)
- Dates are in YYYY-MM-DD format
- The `is_missing_chargeback_data` flag is only in `int_payment_transactions`, not in the marts
- If you need transaction-level detail for Q1 or Q2, use `int_payment_transactions` instead of the marts
- The marts are rebuilt on each `dbt run`, so they'll always reflect the latest seed data

That's it! The models should handle all three questions. Let me know if you run into any issues.

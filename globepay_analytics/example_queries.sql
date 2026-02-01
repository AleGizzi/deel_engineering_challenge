-- Example queries for Globepay Analytics models
-- These demonstrate common analytical use cases

-- 1. Top 10 countries by net revenue
SELECT 
    country,
    currency,
    total_net_revenue_usd,
    acceptance_rate_pct,
    chargeback_rate_pct
FROM {{ ref('mart_country_metrics') }}
ORDER BY total_net_revenue_usd DESC
LIMIT 10;

-- 2. Daily transaction trends (last 30 days)
SELECT 
    transaction_date,
    total_transactions,
    accepted_transactions,
    daily_net_revenue_usd,
    acceptance_rate_pct
FROM {{ ref('mart_daily_metrics') }}
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY transaction_date DESC;

-- 3. Countries with highest chargeback rates
SELECT 
    country,
    currency,
    chargeback_transactions,
    total_net_revenue_usd,
    chargeback_rate_pct
FROM {{ ref('mart_country_metrics') }}
WHERE chargeback_transactions > 0
ORDER BY chargeback_rate_pct DESC
LIMIT 10;

-- 4. Daily summary by country (specific date range)
SELECT 
    transaction_date,
    country,
    currency,
    total_transactions,
    accepted_transactions,
    gross_revenue_usd,
    net_revenue_usd,
    acceptance_rate_pct
FROM {{ ref('mart_payment_summary') }}
WHERE transaction_date BETWEEN '2019-01-01' AND '2019-01-31'
ORDER BY transaction_date, country, currency;

-- 5. Currency performance comparison
SELECT 
    currency,
    COUNT(DISTINCT country) as countries_count,
    SUM(total_transactions) as total_transactions,
    SUM(accepted_transactions) as total_accepted,
    SUM(total_net_revenue_usd) as total_revenue,
    AVG(acceptance_rate_pct) as avg_acceptance_rate,
    AVG(chargeback_rate_pct) as avg_chargeback_rate
FROM {{ ref('mart_country_metrics') }}
GROUP BY currency
ORDER BY total_revenue DESC;

-- 6. Weekly aggregation (using daily metrics)
SELECT 
    DATE_TRUNC('week', transaction_date) as week_start,
    SUM(total_transactions) as weekly_transactions,
    SUM(accepted_transactions) as weekly_accepted,
    SUM(daily_net_revenue_usd) as weekly_revenue,
    AVG(acceptance_rate_pct) as avg_acceptance_rate
FROM {{ ref('mart_daily_metrics') }}
GROUP BY DATE_TRUNC('week', transaction_date)
ORDER BY week_start DESC;

-- 7. Transaction details with chargeback flag
SELECT 
    external_ref,
    transaction_date,
    country,
    currency,
    state,
    amount_usd,
    is_chargeback,
    CASE 
        WHEN is_chargeback THEN 'Yes'
        ELSE 'No'
    END as has_chargeback
FROM {{ ref('int_payment_transactions') }}
WHERE state = 'ACCEPTED'
ORDER BY transaction_date DESC, amount_usd DESC
LIMIT 100;

-- 8. CVV impact analysis
SELECT 
    country,
    cvv_provided,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) as accepted_count,
    CAST(SUM(CASE WHEN is_accepted THEN 1 ELSE 0 END) AS DECIMAL) / 
        NULLIF(COUNT(*), 0) * 100 as acceptance_rate_pct
FROM {{ ref('int_payment_transactions') }}
GROUP BY country, cvv_provided
ORDER BY country, cvv_provided;

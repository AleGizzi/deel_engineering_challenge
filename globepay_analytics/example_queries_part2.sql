-- Part 2: Production-Ready Model for Data Analyst
-- These queries answer the three required questions


-- QUESTION 1: What is the acceptance rate over time?

SELECT
    transaction_date,
    acceptance_rate_pct,
    accepted_transactions,
    declined_transactions,
    total_transactions
FROM {{ ref('mart_daily_metrics') }}
ORDER BY transaction_date;




-- QUESTION 2: List the countries where the amount of declined transactions went over $25M

SELECT
    country,
    currency,
    total_declined_amount_usd,
    declined_transactions,
    total_transactions,
    decline_rate_pct,
    -- Format amount for readability
    round(total_declined_amount_usd / 1000000, 2) as declined_amount_millions
FROM {{ ref('mart_country_metrics') }}
WHERE total_declined_amount_usd > 25000000
ORDER BY total_declined_amount_usd DESC;




-- QUESTION 3: Which transactions are missing chargeback data?

SELECT
    external_ref,
    transaction_timestamp,
    transaction_date,
    country,
    currency,
    state,
    amount_usd,
    is_missing_chargeback_data,
    -- Additional context
    case 
        when state = 'ACCEPTED' then 'Accepted - Missing Chargeback Data'
        when state = 'DECLINED' then 'Declined - Missing Chargeback Data'
        else 'Unknown State'
    end as transaction_status_note
FROM {{ ref('int_payment_transactions') }}
WHERE is_missing_chargeback_data = true
ORDER BY transaction_date DESC, amount_usd DESC;


-- Mart model - high-level payment summary metrics
-- Aggregated by date, country, currency, and transaction status

with transactions as (
    select * from {{ ref('int_payment_transactions') }}
),

daily_summary as (
    select
        transaction_date,
        country,
        currency,

        -- Transaction counts
        count(*) as total_transactions,
        sum(case when is_accepted then 1 else 0 end) as accepted_transactions,
        sum(case when is_declined then 1 else 0 end) as declined_transactions,
        sum(case when is_chargeback then 1 else 0 end) as chargeback_transactions,

        -- Revenue metrics
        sum(gross_revenue_usd) as gross_revenue_usd,
        sum(net_revenue_usd) as net_revenue_usd,
        sum(case when is_chargeback then amount_usd else 0 end) as chargeback_amount_usd,

        -- Average transaction amounts
        avg(case when is_accepted then amount_usd end) as avg_accepted_amount_usd,
        avg(amount_usd) as avg_transaction_amount_usd,

        -- Acceptance rate
        cast(sum(case when is_accepted then 1 else 0 end) as decimal) 
        / 
        nullif(count(*), 0) * 100 as acceptance_rate_pct,

        -- Chargeback rate (of accepted transactions)
        cast(sum(case when is_chargeback then 1 else 0 end) as decimal) 
        / 
        nullif(sum(case when is_accepted then 1 else 0 end), 0) * 100 as chargeback_rate_pct,

        
    from transactions
    group by 
        transaction_date,
        country,
        currency
)

select * from daily_summary

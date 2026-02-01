-- Mart model - daily payment metrics
-- Time-series analysis of payment 

with transactions as (
    select * from {{ ref('int_payment_transactions') }}
),

daily_metrics as (
    select
        transaction_date,

        -- Transaction counts
        count(*) as total_transactions,
        sum(case when is_accepted then 1 else 0 end) as accepted_transactions,
        sum(case when is_declined then 1 else 0 end) as declined_transactions,
        sum(case when is_chargeback then 1 else 0 end) as chargeback_transactions,

        -- Revenue metrics
        sum(gross_revenue_usd) as daily_gross_revenue_usd,
        sum(net_revenue_usd) as daily_net_revenue_usd,
        sum(case when is_chargeback then amount_usd else 0 end) as daily_chargeback_amount_usd,

        -- Average transaction amounts
        avg(case when is_accepted then amount_usd end) as avg_accepted_amount_usd,
        avg(amount_usd) as avg_transaction_amount_usd,

        -- Rates
        cast(sum(case when is_accepted then 1 else 0 end) as decimal) 
        / 
        nullif(count(*), 0) * 100 as acceptance_rate_pct,
        
        cast(sum(case when is_declined then 1 else 0 end) as decimal) 
        /
        nullif(count(*), 0) * 100 as decline_rate_pct,
        
        cast(sum(case when is_chargeback then 1 else 0 end) as decimal) 
        / 
        nullif(sum(case when is_accepted then 1 else 0 end), 0) * 100 as chargeback_rate_pct,

        -- Country and currency diversity
        count(distinct country) as unique_countries,
        count(distinct currency) as unique_currencies
        
    from transactions
    group by 
        transaction_date
)

select * from daily_metrics
order by transaction_date

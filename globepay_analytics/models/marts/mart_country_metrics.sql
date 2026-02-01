-- Mart model - country-level payment 
-- Aggregated by country 

with transactions as (
    select 
    * 
    from {{ ref('int_payment_transactions') }}
),

country_metrics as (
    select
        country,
        currency,

        -- Transaction counts
        count(*) as total_transactions,
        sum(
            case 
                when is_accepted 
                then 1 
                else 0 
            end
            ) as accepted_transactions,

        sum(
            case 
                when is_declined 
                then 1 
                else 0 
            end
            ) as declined_transactions,

        sum(
            case 
                when is_chargeback 
                then 1 
                else 0 
            end
            ) as chargeback_transactions,

        -- Revenue metrics
        sum(gross_revenue_usd) as total_gross_revenue_usd,
        sum(net_revenue_usd) as total_net_revenue_usd,
        sum(
            case 
                when is_chargeback 
                then amount_usd 
                else 0 
            end
            ) as total_chargeback_amount_usd,

        -- Declined transaction amounts
        sum(
            case 
                when is_declined 
                then amount_usd 
                else 0 
            end
            ) as total_declined_amount_usd,

        -- Average transaction amounts
        avg(
            case 
                when is_accepted 
                then amount_usd 
            end
            ) as avg_accepted_amount_usd,

        avg(amount_usd) as avg_transaction_amount_usd,
        min(
            case 
                when is_accepted 
                then amount_usd 
            end
            ) as min_accepted_amount_usd,

        max(
            case 
                when is_accepted 
                then amount_usd 
            end
            ) as max_accepted_amount_usd,

        -- Rates
        cast( sum( case  when is_accepted then 1 else 0 end ) as decimal ) 
        / 
        nullif(count(*), 0) * 100  as acceptance_rate_pct,


        cast(sum(case when is_declined then 1 else 0 end) as decimal) 
        / 
        nullif(count(*), 0) * 100 as decline_rate_pct,


        cast(sum(case when is_chargeback then 1 else 0 end) as decimal) 
        / 
        nullif(sum(case when is_accepted then 1 else 0 end), 0) * 100 as chargeback_rate_pct,

        -- CVV metrics
        sum(case when cvv_provided then 1 else 0 end) as transactions_with_cvv,

        cast(sum(case when cvv_provided and is_accepted then 1 else 0 end) as decimal) 
        / 
        nullif(sum(case when cvv_provided then 1 else 0 end), 0) * 100 as cvv_acceptance_rate_pct,

        -- Date range
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date

    from transactions
    group by 
        country,
        currency
)

select 
* 
from country_metrics
order by total_net_revenue_usd desc

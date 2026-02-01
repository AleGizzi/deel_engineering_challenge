-- Combines acceptance and chargeback data
-- View of all payment transactions

with acceptance as (
    select * from {{ ref('stg_globepay__acceptance_report') }}
),

chargeback as (
    select 
        external_ref,
        is_chargeback
    from {{ ref('stg_globepay__chargeback_report') }}
),

joined as (
    select
        acceptance.*,
        chargeback.is_chargeback as raw_is_chargeback,
        coalesce(chargeback.is_chargeback, false) as is_chargeback,
        
        -- Flag transactions missing chargeback data
        case when chargeback.external_ref is null then true else false end as is_missing_chargeback_data
    from acceptance
    left join chargeback
        on acceptance.external_ref = chargeback.external_ref
),

final as (
    select
        *,

        -- Calculate transaction status flags
        case when state = 'ACCEPTED' then true else false end as is_accepted,
        case when state = 'DECLINED' then true else false end as is_declined,

        -- Calculate net revenue (accepted transactions only, excluding chargebacks)
        case 
            when state = 'ACCEPTED' and not coalesce(is_chargeback, false) 
            then amount_usd 
            else 0 
        end as net_revenue_usd,

        -- Calculate gross revenue (all accepted transactions)
        case 
            when state = 'ACCEPTED' 
            then amount_usd 
            else 0 
        end as gross_revenue_usd

    from joined
)

select * from final

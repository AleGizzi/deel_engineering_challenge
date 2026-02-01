-- Staging model for Globepay Acceptance Report


with source as (
    select * from {{ ref('acceptance_report') }}
),

renamed as (
    select
        external_ref,
        case when status = 'TRUE' then true else false end as is_active,
        source,
        ref,
        cast(date_time as timestamp) as transaction_timestamp,
        state,
        case when cvv_provided = 'TRUE' then true else false end as cvv_provided,
        cast(amount as decimal(18, 2)) as amount,
        country,
        currency,
        cast(rates as json) as exchange_rates
    from source
),

final as (
    select
        external_ref,
        is_active,
        source,
        ref,
        transaction_timestamp,
        state,
        cvv_provided,
        amount,
        country,
        currency,
        exchange_rates,

        -- Extract date components for easier filtering
        date(transaction_timestamp) as transaction_date,
        extract(year from transaction_timestamp) as transaction_year,
        extract(month from transaction_timestamp) as transaction_month,
        extract(dow from transaction_timestamp) as transaction_day_of_week,

        -- Parse exchange rates to USD 
        cast(json_extract(exchange_rates, '$.USD') as decimal) as usd_rate,
        cast(json_extract(exchange_rates, '$.EUR') as decimal) as eur_rate,
        cast(json_extract(exchange_rates, '$.GBP') as decimal) as gbp_rate,
        cast(json_extract(exchange_rates, '$.CAD') as decimal) as cad_rate,
        cast(json_extract(exchange_rates, '$.MXN') as decimal) as mxn_rate,

        -- Calculate amount in USD
        case 
            when currency = 'USD' then amount
            when currency = 'EUR' then amount / nullif(cast(json_extract(exchange_rates, '$.EUR') as decimal), 0)
            when currency = 'GBP' then amount / nullif(cast(json_extract(exchange_rates, '$.GBP') as decimal), 0)
            when currency = 'CAD' then amount / nullif(cast(json_extract(exchange_rates, '$.CAD') as decimal), 0)
            when currency = 'MXN' then amount / nullif(cast(json_extract(exchange_rates, '$.MXN') as decimal), 0)
            else amount / nullif(cast(json_extract(exchange_rates, '$.USD') as decimal), 0)
        end as amount_usd

    from renamed
)

select * from final

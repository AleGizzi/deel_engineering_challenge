-- Staging model for Globepay Chargeback Report


with source as (
    select * from {{ ref('chargeback_report') }}
),

renamed as (
    select
        external_ref,
        case when status = 'TRUE' then true else false end as is_active,
        source,
        case when chargeback = 'TRUE' then true else false end as is_chargeback
    from source
),

final as (
    select
        external_ref,
        is_active,
        source,
        is_chargeback
    from renamed
)

select * from final

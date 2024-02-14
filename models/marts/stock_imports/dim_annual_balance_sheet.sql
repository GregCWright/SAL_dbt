with source as (
    select
        *
    from {{ ref("intermediate__annual_balance_sheet") }}
)

select * from source

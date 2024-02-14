with source as (
    select
        *
    from {{ ref("intermediate__quarterly_balance_sheet") }}
)

select * from source

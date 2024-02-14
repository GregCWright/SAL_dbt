with source as (
    select
        *
    from {{ ref("intermediate__quarterly_cash_flow") }}
)

select * from source

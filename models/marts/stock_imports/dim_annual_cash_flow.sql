with source as (
    select
        *
    from {{ ref("intermediate__annual_cash_flow") }}
)

select * from source

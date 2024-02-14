with source as (
    select
        *
    from {{ ref("intermediate__annual_income_statement") }}
)

select * from source

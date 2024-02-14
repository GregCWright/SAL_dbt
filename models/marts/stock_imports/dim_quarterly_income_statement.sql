with source as (
    select
        *
    from {{ ref("intermediate__quarterly_income_statement") }}
)

select * from source

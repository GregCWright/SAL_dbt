with source as (
    select
        *
    from landing.quarterly_income_statement
)

select * from source

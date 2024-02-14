with source as (
    select
        *
    from landing.quarterly_balance_sheet
)

select * from source

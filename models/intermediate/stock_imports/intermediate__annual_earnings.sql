with source as (
    select
        *
    from {{ ref("test_annual_earnings") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_number('reported_earnings_per_share', 'float') }} as reported_earnings_per_share
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

select * from standardized

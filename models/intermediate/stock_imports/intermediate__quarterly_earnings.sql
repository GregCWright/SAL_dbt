with source as (
    select
        *
    from {{ ref("test_quarterly_earnings") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_date('reported_date') }} as reported_date
        , {{ standardize_number('reported_earnings_per_share', 'float') }} as reported_earnings_per_share
        , {{ standardize_number('estimated_earnings_per_share', 'float') }} as estimated_earnings_per_share
        , {{ standardize_number('surprise', 'float') }} as surprise
        , {{ standardize_number('surprise_percentage', 'float') }} as surprise_percentage
        , {{ standardize_datetime('execuation_time') }} as execution_time
    from source
)

select * from standardized
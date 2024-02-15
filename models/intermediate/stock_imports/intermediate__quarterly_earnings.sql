with source as (
    select
        *
    from {{ ref("source__quarterly_earnings") }}
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
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

, deduped as (
    select
        fiscal_date_ending
        , symbol
        , reported_date
        , reported_earnings_per_share
        , estimated_earnings_per_share
        , surprise
        , surprise_percentage
        , execution_time
    from {{ dedupe_multiple("standardized", "fiscal_date_ending", "symbol", "execution_time") }}
)

select * from deduped

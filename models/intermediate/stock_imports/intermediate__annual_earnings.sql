with source as (
    select
        *
    from {{ ref("source__annual_earnings") }}
)

, standardized as (
    select
        {{ standardize_date('fiscal_date_ending') }} as fiscal_date_ending
        , {{ standardize_varchar('symbol', 5) }} as symbol
        , {{ standardize_number('reported_earnings_per_share', 'float') }} as reported_earnings_per_share
        , {{ standardize_datetime('execution_time') }} as execution_time
    from source
)

, deduped as (
    select
        fiscal_date_ending
        , symbol
        , reported_earnings_per_share
        , execution_time
    from {{ dedupe_multiple("standardized", "fiscal_date_ending", "symbol", "execution_time") }}
)

select * from deduped

with source as (
    select
        *
    from {{ ref("source__time_series_daily") }}
)

, standardized as (
    select
        {{ standardize_date('daily_price_date') }}      as daily_price_date
        , {{ standardize_varchar('symbol', 5) }}        as symbol
        , {{ standardize_number('open', 'float') }}     as open
        , {{ standardize_number('high', 'float') }}     as high
        , {{ standardize_number('low', 'float') }}      as low
        , {{ standardize_number('close', 'float') }}    as close
        , {{ standardize_number('volume', 'bigint') }}  as volume
        , {{ standardize_datetime('execution_time') }}  as execution_time
    from source
)

, deduped as (
    select
        daily_price_date
        , symbol
        , open
        , high
        , low
        , close
        , volume
        , execution_time
    from {{ dedupe_multiple("standardized", "daily_price_date", "symbol", "execution_time") }}
)

select * from deduped

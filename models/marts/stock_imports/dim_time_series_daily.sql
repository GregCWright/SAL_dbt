with source as (
    select
        *
    from {{ ref("intermediate__time_series_daily") }}
)

select * from source

with source as (
    select
        *
    from landing.time_series_daily
)

select * from source

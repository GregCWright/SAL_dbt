with source as (
    select
        *
    from {{ ref("intermediate__quarterly_earnings") }}
)

select * from source

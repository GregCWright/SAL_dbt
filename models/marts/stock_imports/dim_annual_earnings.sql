with source as (
    select
        *
    from {{ ref("intermediate__annual_earnings") }}
)

select * from source

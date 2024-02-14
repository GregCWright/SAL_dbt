with source as (
    select
        *
    from {{ ref("intermediate__overview") }}
)

select * from source

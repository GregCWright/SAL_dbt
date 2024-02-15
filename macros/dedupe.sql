{% macro dedupe_multiple(table_name, first_column, second_column, date_column) %}
    (
        select
            *
            , row_number() over( partition by {{ first_column }}, {{ second_column }} order by {{ date_column }} desc ) AS row_number
        from {{ table_name }}
    ) as row_query
    where row_number = 1
    
{% endmacro %}
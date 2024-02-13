{% macro standardize_date(column_name) %}
    {{ standardize_varchar(column_name, 32) }}::date
{% endmacro %}
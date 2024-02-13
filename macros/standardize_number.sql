{% macro standardize_number(column_name, type, expression='[^a-zA-Z0-9.-]+') %}
    {{ standardize_varchar(column_name) }}::{{ type }}
{% endmacro %}
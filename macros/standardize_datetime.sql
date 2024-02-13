{% macro standardize_datetime(column_name, format='YYYY-MM-DD HH24:MI:SS.US ZZZ') %}
    to_timestamp({{ column_name }}, '{{ format }}')::timestamptz
{% endmacro %}
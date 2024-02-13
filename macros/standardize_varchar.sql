{% macro standardize_varchar(column_name, size=32, expression='[^\w. ]+') %}
    case 
        when cast({{ column_name }} as varchar({{ size }})) = '"None"' then null
        else lower(regexp_replace(cast({{ column_name }} as varchar({{ size }})), '{{ expression }}', '','g'))
    end
{% endmacro %}
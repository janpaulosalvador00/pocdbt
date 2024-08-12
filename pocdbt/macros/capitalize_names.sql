-- macros/capitalize_names.sql
{% macro capitalize_names(column_name) %}
    INITCAP({{ column_name }})
{% endmacro %}

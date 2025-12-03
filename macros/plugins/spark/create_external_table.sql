{% macro spark__create_external_table(source_node) %}

    {%- set columns = source_node.columns.values() -%}
    {%- set external = source_node.external -%}
    {%- set raw_partitions = external.partitions -%}
    {%- set options = external.options -%}

    {# Coerce partitions strings (dbt 1.7 + spark) back into mappings #}
    {%- set partitions = [] -%}
    {%- if raw_partitions -%}
        {%- for partition in raw_partitions -%}
            {%- if partition is string -%}
                {%- set parsed = fromyaml(partition) -%}
                {%- if parsed is mapping -%}
                    {% do partitions.append(parsed) %}
                {%- elif parsed is sequence -%}
                    {%- for item in parsed -%}
                        {% do partitions.append(item) %}
                    {%- endfor -%}
                {%- endif -%}
            {%- else -%}
                {% do partitions.append(partition) %}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

{# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html #}
    create table {{source(source_node.source_name, source_node.name)}} 
    {%- if columns|length > 0 %} (
        {% for column in columns %}
            {{column.name}} {{column.data_type}}
            {{- ',' if not loop.last -}}
        {% endfor %}
    ) {% endif -%}
    {% if external.using %} using {{external.using}} {%- endif %}
    {% if options -%} options (
        {%- for key, value in options.items() -%}
            '{{ key }}' = '{{value}}' {{- ', \n' if not loop.last -}}
        {%- endfor -%}
    ) {%- endif %}
    {% if partitions -%} partitioned by (
        {%- for partition in partitions -%}
            {{partition.name}} {{partition.data_type}}{{', ' if not loop.last}}
        {%- endfor -%}
    ) {%- endif %}
    {% if external.row_format -%} row format {{external.row_format}} {%- endif %}
    {% if external.file_format -%} stored as {{external.file_format}} {%- endif %}
    {% if external.location -%} location '{{external.location}}' {%- endif %}
    {% if external.table_properties -%} tblproperties {{ external.table_properties }} {%- endif -%}

{% endmacro %}

{% macro remove_soft_deletes() -%}

    {% set sources = [] -%}

    {% if execute %}
        {% for node in graph.sources.values() -%}
            {% if 'has_soft_deletes' in node.tags %}
                {% if '_soft_deleted' in dbt_utils.get_filtered_columns_in_relation(from=source(node.source_name, node.name)) %}
                    {%- do sources.append(source(node.source_name, node.name)) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor %}
    {% endif %}
    
    {% set query %}  
        begin;
        {%- for source in sources %}
            delete from {{ source }} where _soft_deleted;
        {% endfor %}
        commit;
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}

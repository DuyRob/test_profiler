{% macro get_sources(source_name) %}
    {% set sources = [] %}
    {% if execute %}
    {% for node in graph.sources.values() %}
        {% if node.source_name == source_name %}
            {% set new_source = source(node.source_name, node.name) %}
            {% do sources.append(new_source) %}
        {% endif %}
    {% endfor %}
    {% do return(sources) %}
    {% endif %}
{% endmacro %}

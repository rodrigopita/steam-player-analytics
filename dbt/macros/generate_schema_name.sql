-- Overrides dbt's default, which builds <target_schema>_<custom_schema> (marts
-- would land in staging_marts). The prefix exists so multiple developers sharing
-- a database don't clobber each other's builds; this project is one developer,
-- one local warehouse, so custom schemas are used as-is: staging and marts read
-- straight off `\dn`. If real environments ever appear, switch to the built-in
-- generate_schema_name_for_env instead of re-adding the prefix.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

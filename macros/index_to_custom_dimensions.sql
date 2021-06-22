{% macro index_to_custom_dimensions(cd_index) -%}
(SELECT value FROM hits.customdimensions WHERE index = {{ cd_index }} LIMIT 1) AS customDim{{ cd_index }},
{%- endmacro %}
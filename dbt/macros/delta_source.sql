{#
    The one macro every staging model calls instead of hardcoding a path.
    Deliberately engine-aware: DuckDB reads a Delta table directly off
    S3-compatible storage via its `delta` extension's `delta_scan()`
    table function (see profiles.yml for the extension/secret setup);
    Snowflake has no equivalent zero-copy Delta reader built in, so the
    honest Snowflake path is an external table pre-created over the same
    S3 location (documented in profiles.yml, not faked here).

    This is the actual boundary of "Snowflake-portable dbt": every mart
    model downstream of staging is plain adapter-agnostic SQL/Jinja and
    needs zero changes to run on Snowflake. Only staging's OWN source
    definition is engine-specific, and it's isolated to this one macro
    plus 9 one-line staging models -- not scattered through the project.

    A third mode, `--vars '{use_seeds: true}'`: CI doesn't have live
    Postgres/MinIO/Spark infrastructure to build real bronze/silver
    tables from, and spinning up the full stack in a GitHub Actions
    runner just to lint SQL would be slow and heavy for what a PR check
    needs. Small hand-written CSV fixtures under dbt/seeds/ (matching
    the real silver-layer shapes) stand in for delta_source() instead --
    same models, same tests, same dbt build, real infrastructure
    swapped for fast/deterministic fixtures. See .github/workflows/ci.yml.
#}
{% macro delta_source(layer, table_name) %}
    {%- if var('use_seeds', false) -%}
        {{ ref(table_name) }}
    {%- elif target.type == 'duckdb' -%}
        delta_scan('s3://{{ var("lakehouse_bucket") }}/{{ layer }}/{{ table_name }}')
    {%- elif target.type == 'snowflake' -%}
        {{ source('lakehouse_external', table_name) }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("delta_source(): no implementation for target.type = " ~ target.type) }}
    {%- endif -%}
{% endmacro %}

"""
Generates docs/DATA_DICTIONARY.md from dbt's own build artifacts --
`target/catalog.json` (real column names/types, introspected from the
actual warehouse after a build) and `target/manifest.json` (model/column
descriptions, tests, and the resolved DAG) -- rather than hand-maintaining
a separate document that drifts from the real schema the moment a
column gets renamed. This is the standard "docs as a build artifact, not
a wiki page" pattern: if this document is stale, `dbt build` + this
script fixes it, not an editor.

Column-level lineage (which upstream model/source a gold column
ultimately traces back to) isn't in dbt's raw artifacts directly --
manifest.json has the model-to-model DAG (what feeds what) but not
per-column provenance. Rather than hand-fake it, this pulls the real
per-column test coverage (not_null/unique/relationships/accepted_values)
straight out of manifest.json, which is the governance signal that
actually matters for a data dictionary: not just "what does this column
mean" but "what's actually enforced about it."

Run (needs `dbt docs generate` to have populated target/ first):
    docker compose --profile orchestration exec airflow-scheduler bash -c
      "cd /opt/airflow/dbt && dbt docs generate && python /opt/airflow/governance/generate_data_dictionary.py"
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

DBT_TARGET = Path("/opt/airflow/dbt/target")
OUTPUT_PATH = Path("/opt/airflow/docs/DATA_DICTIONARY.md")

LAYER_ORDER = ["staging", "marts"]
LAYER_LABELS = {"staging": "Staging (silver passthrough)", "marts": "Marts (gold: dims, facts, measures)"}


def load_json(name: str) -> dict:
    with open(DBT_TARGET / name) as f:
        return json.load(f)


def collect_column_tests(manifest: dict) -> dict[str, dict[str, list[str]]]:
    """model_unique_id -> {column_name: [test descriptions]}"""
    tests_by_column: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test":
            continue
        test_meta = node.get("test_metadata")
        column_name = node.get("column_name")
        depends_on = node.get("depends_on", {}).get("nodes", [])
        if not column_name or not depends_on:
            continue
        parent = depends_on[0]
        if test_meta:
            name = test_meta.get("name", node["name"])
            kwargs = test_meta.get("kwargs", {})
            if name == "relationships":
                label = f"relationships -> {kwargs.get('to', '?')}.{kwargs.get('field', '?')}"
            elif name == "accepted_values":
                label = f"accepted_values {kwargs.get('values', [])}"
            else:
                label = name
        else:
            label = node["name"].split(".")[0]
        tests_by_column[parent][column_name].append(label)
    return tests_by_column


def main() -> None:
    manifest = load_json("manifest.json")
    catalog = load_json("catalog.json")

    tests_by_column = collect_column_tests(manifest)

    models_by_layer: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for unique_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        layer = "staging" if node["name"].startswith("stg_") else "marts"
        models_by_layer[layer].append((unique_id, node))

    lines: list[str] = []
    lines.append("# Data Dictionary")
    lines.append("")
    lines.append(
        "Generated from `dbt/target/catalog.json` + `dbt/target/manifest.json` by "
        "`governance/generate_data_dictionary.py` -- **do not hand-edit**; regenerate after "
        "`dbt docs generate` instead. Column types are introspected from the real, built "
        "warehouse, not asserted; test coverage shown is what's actually enforced by CI/the "
        "quality gate, not a claim."
    )
    lines.append("")
    lines.append(
        f"Generated against {len(manifest['nodes'])} manifest nodes, "
        f"{sum(len(v) for v in models_by_layer.values())} models."
    )
    lines.append("")

    for layer in LAYER_ORDER:
        models = sorted(models_by_layer.get(layer, []), key=lambda x: x[1]["name"])
        if not models:
            continue
        lines.append(f"## {LAYER_LABELS[layer]}")
        lines.append("")

        for unique_id, node in models:
            name = node["name"]
            description = node.get("description", "").strip() or "_(no description)_"
            lines.append(f"### `{name}`")
            lines.append("")
            lines.append(description)
            lines.append("")

            catalog_entry = catalog.get("nodes", {}).get(unique_id)
            if not catalog_entry:
                lines.append("_(not present in catalog -- run `dbt build` before `dbt docs generate`)_")
                lines.append("")
                continue

            lines.append("| Column | Type | Description | Enforced |")
            lines.append("|---|---|---|---|")
            catalog_columns = catalog_entry.get("columns", {})
            manifest_columns = node.get("columns", {})
            column_tests = tests_by_column.get(unique_id, {})

            # catalog.json preserves warehouse column order (index-sorted); manifest may know more (docs) than catalog does for computed/undocumented columns
            ordered_cols = sorted(catalog_columns.items(), key=lambda kv: kv[1].get("index", 999))
            seen = set()
            for col_name, col_info in ordered_cols:
                seen.add(col_name.lower())
                doc = manifest_columns.get(col_name, {}).get("description", "").strip()
                enforced = ", ".join(column_tests.get(col_name, [])) or "-"
                dtype = col_info.get("type", "?")
                lines.append(f"| `{col_name}` | {dtype} | {doc or '-'} | {enforced} |")

            lines.append("")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("\n".join(lines))
    print(f"wrote {OUTPUT_PATH} ({len(lines)} lines)")


if __name__ == "__main__":
    main()

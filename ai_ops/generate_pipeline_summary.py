import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


AI_OPS_DIR = Path(__file__).resolve().parent
CONTEXT_PATH = AI_OPS_DIR / "pipeline_context.json"
REPORT_PATH = AI_OPS_DIR.parent / "reports" / "latest_pipeline_summary.md"


CRITICAL_TABLES = {
    "raw_fakestore.products",
    "pprojects_488010_marketing_data.deals",
    "dbt_kachiokeke.stg_fakestore_products",
    "dbt_kachiokeke.mart_product_catalog",
    "dbt_kachiokeke.stg_hubspot_deals",
    "dbt_kachiokeke.int_hubspot_deals_enriched",
    "dbt_kachiokeke.mart_sales_pipeline",
    "dbt_kachiokeke.mart_sales_pipeline_monthly",
}


def load_context() -> dict[str, Any]:
    if not CONTEXT_PATH.exists():
        raise FileNotFoundError(
            f"Missing context file: {CONTEXT_PATH}. "
            "Run collect_pipeline_context.py first."
        )

    return json.loads(CONTEXT_PATH.read_text(encoding="utf-8"))


def table_lookup(context: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        table["name"]: table
        for table in context.get("tables", [])
    }


def row_count(tables: dict[str, dict[str, Any]], table_name: str) -> Optional[int]:
    value = tables.get(table_name, {}).get("row_count")

    if value is None:
        return None

    return int(value)


def has_rows(tables: dict[str, dict[str, Any]], table_name: str) -> bool:
    count = row_count(tables, table_name)
    return count is not None and count > 0


def is_zero_or_missing(tables: dict[str, dict[str, Any]], table_name: str) -> bool:
    count = row_count(tables, table_name)
    return count is None or count == 0


def build_warnings(context: dict[str, Any]) -> list[str]:
    tables = table_lookup(context)
    warnings = []

    for table_name in sorted(CRITICAL_TABLES):
        table = tables.get(table_name, {})

        if not table.get("exists"):
            warnings.append(f"Critical table `{table_name}` could not be read.")
            continue

        if table.get("row_count") == 0:
            warnings.append(f"Critical table `{table_name}` has zero rows.")

    if (
        has_rows(tables, "dbt_kachiokeke.mart_product_catalog")
        and is_zero_or_missing(tables, "raw_fakestore.products")
    ):
        warnings.append(
            "`mart_product_catalog` has rows while `raw_fakestore.products` "
            "has zero or unavailable rows."
        )

    hubspot_downstream_tables = [
        "dbt_kachiokeke.stg_hubspot_deals",
        "dbt_kachiokeke.int_hubspot_deals_enriched",
        "dbt_kachiokeke.mart_sales_pipeline",
        "dbt_kachiokeke.mart_sales_pipeline_monthly",
    ]

    if (
        any(has_rows(tables, table_name) for table_name in hubspot_downstream_tables)
        and is_zero_or_missing(tables, "pprojects_488010_marketing_data.deals")
    ):
        warnings.append(
            "One or more HubSpot downstream models have rows while the raw "
            "HubSpot deals source has zero or unavailable rows."
        )

    if (
        has_rows(tables, "snapshots.snap_hubspot_deals")
        and is_zero_or_missing(tables, "pprojects_488010_marketing_data.deals")
    ):
        warnings.append(
            "`snap_hubspot_deals` has rows while the raw HubSpot deals source "
            "has zero or unavailable rows."
        )

    latest_run = context.get("latest_ingestion_run", {})
    latest_record = latest_run.get("record") or {}

    if latest_run.get("error"):
        warnings.append(
            "Latest ingestion run could not be collected from "
            "`raw_fakestore.ingestion_run_log`."
        )
    elif latest_record.get("status") and latest_record.get("status") != "success":
        warnings.append(
            "Latest Fake Store ingestion run did not complete successfully."
        )

    return warnings


def determine_status(warnings: list[str]) -> str:
    if warnings:
        return "Needs attention"

    return "Healthy based on available row-count checks"


def format_value(value: Any) -> str:
    if value is None:
        return "Unavailable"

    return str(value)


def render_row_counts(context: dict[str, Any]) -> list[str]:
    lines = [
        "| Table | Layer | Exists | Row Count |",
        "|---|---:|---:|---:|",
    ]

    for table in context.get("tables", []):
        exists = "Yes" if table.get("exists") else "No"
        lines.append(
            "| "
            f"`{table.get('name')}` | "
            f"{table.get('layer')} | "
            f"{exists} | "
            f"{format_value(table.get('row_count'))} |"
        )

    return lines


def render_latest_ingestion_run(context: dict[str, Any]) -> list[str]:
    latest_run = context.get("latest_ingestion_run", {})
    record = latest_run.get("record")

    if latest_run.get("error"):
        return [
            "Latest ingestion run was unavailable.",
            "",
            f"Error: `{latest_run['error']}`",
        ]

    if not record:
        return ["No ingestion run record was available."]

    return [
        f"- Run ID: `{format_value(record.get('run_id'))}`",
        f"- Source: `{format_value(record.get('source_name'))}`",
        f"- Target table: `{format_value(record.get('target_table'))}`",
        f"- Status: `{format_value(record.get('status'))}`",
        f"- Rows loaded: `{format_value(record.get('rows_loaded'))}`",
        f"- Started at: `{format_value(record.get('started_at'))}`",
        f"- Completed at: `{format_value(record.get('completed_at'))}`",
    ]


def render_source_health(context: dict[str, Any]) -> list[str]:
    tables = table_lookup(context)
    source_names = [
        "raw_fakestore.products",
        "raw_fakestore.ingestion_run_log",
        "pprojects_488010_marketing_data.deals",
    ]

    lines = []

    for source_name in source_names:
        table = tables.get(source_name, {})

        if not table.get("exists"):
            lines.append(f"- `{source_name}`: unavailable")
        elif table.get("row_count") == 0:
            lines.append(f"- `{source_name}`: available but empty")
        else:
            lines.append(
                f"- `{source_name}`: available with {table.get('row_count')} rows"
            )

    return lines


def recommended_actions(warnings: list[str]) -> list[str]:
    if not warnings:
        return [
            "- Continue using dbt tests, source freshness, and validation as the primary quality gates.",
            "- Review this summary after scheduled runs to confirm source-to-mart row movement.",
            "- Consider routing this Markdown report to Slack or email for lightweight monitoring.",
        ]

    return [
        "- Run the existing dbt tests and source freshness checks to verify the failing area.",
        "- Confirm the latest ingestion run completed and loaded the expected number of rows.",
        "- Check whether missing or zero-row tables are expected for the current environment.",
        "- Use this report as triage context, then rely on dbt/BigQuery checks for root-cause validation.",
    ]


def llm_rewrite_placeholder(markdown_report: str) -> str:
    """
    Placeholder for a future LLM rewrite step.

    Keep the default report deterministic and dependency-free. A later version
    could pass markdown_report to an LLM to improve tone or produce an executive
    summary, but the rule-based content should remain the source of truth.
    """
    return markdown_report


def render_report(context: dict[str, Any]) -> str:
    warnings = build_warnings(context)
    status = determine_status(warnings)

    generated_at = datetime.now(timezone.utc).isoformat()
    context_generated_at = context.get("generated_at", "Unavailable")

    warning_lines = [f"- {warning}" for warning in warnings]
    if not warning_lines:
        warning_lines = ["- No warnings from deterministic row-count rules."]

    sections = [
        "# Latest Pipeline Summary",
        "",
        f"Generated at: `{generated_at}`",
        f"Context collected at: `{context_generated_at}`",
        f"Project: `{context.get('project_id', 'Unavailable')}`",
        "",
        "## Overall Pipeline Status",
        "",
        status,
        "",
        "## Source Health",
        "",
        *render_source_health(context),
        "",
        "## Row Counts by Table",
        "",
        *render_row_counts(context),
        "",
        "## Latest Ingestion Run",
        "",
        *render_latest_ingestion_run(context),
        "",
        "## Warnings",
        "",
        *warning_lines,
        "",
        "## Recommended Next Actions",
        "",
        *recommended_actions(warnings),
        "",
    ]

    deterministic_report = "\n".join(sections)
    return llm_rewrite_placeholder(deterministic_report)


def main() -> None:
    context = load_context()
    report = render_report(context)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report, encoding="utf-8")

    print(f"Wrote pipeline summary to {REPORT_PATH}")


if __name__ == "__main__":
    main()

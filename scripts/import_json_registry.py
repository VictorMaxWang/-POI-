from __future__ import annotations

import argparse
import json
from pathlib import Path

from normalize_registry_evidence import (
    build_derived_source_id,
    extract_rows_from_json_payload,
    materialize_derived_source,
)
from pipeline_common import (
    DOCS_DIR,
    file_sha1,
    get_manifest_row,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")
JSON_EVIDENCE_TYPES = {"json_response_export", "json_response", "json"}


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return DOCS_DIR.parent / path


def append_reason(row: dict[str, str], reason: str) -> None:
    reason = reason.strip()
    if not reason:
        return
    parts = [part.strip() for part in row.get("remark", "").split("|") if part.strip()]
    if reason not in parts:
        parts.append(reason)
    row["remark"] = " | ".join(parts)


def should_process(row: dict[str, str], cities: set[str], source_ids: set[str], manual_ids: set[str]) -> bool:
    if cities and row.get("city") not in cities:
        return False
    if source_ids and row.get("source_id") not in source_ids:
        return False
    if manual_ids and row.get("manual_id") not in manual_ids:
        return False
    evidence_type = row.get("evidence_type", "").strip().lower()
    if evidence_type not in JSON_EVIDENCE_TYPES and row.get("capture_status", "").upper() != "JSON_EXPORTED":
        return False
    return bool(row.get("evidence_file_path", "").strip())


def update_manual_rows(updated_rows: list[dict[str, str]]) -> None:
    write_csv_rows(DOCS_DIR / "manual_capture_template.csv", OUTPUT_FIELDS, updated_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import single JSON response evidence into derived registry sources.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    parser.add_argument("--source-id", nargs="*", default=[], help="Limit to source IDs")
    parser.add_argument("--manual-id", nargs="*", default=[], help="Limit to manual IDs")
    args = parser.parse_args()

    manual_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    imported = 0

    for row in manual_rows:
        if not should_process(row, set(args.city), set(args.source_id), set(args.manual_id)):
            continue

        source_row = get_manifest_row(row.get("source_id", "")) or {
            "source_id": row.get("source_id", ""),
            "city": row.get("city", ""),
            "target_table": "nursery_registry_raw",
            "official_or_platform": "official-linked platform",
            "priority": "1",
        }
        evidence_path = resolve_path(row.get("evidence_file_path", ""))
        if not evidence_path.exists():
            row["import_status"] = "FAILED"
            append_reason(row, f"missing_file:{evidence_path}")
            continue

        evidence_sha1 = file_sha1(evidence_path)
        row["evidence_file_sha1"] = evidence_sha1
        derived_source_id = row.get("derived_source_id", "") or build_derived_source_id(
            row.get("source_id", ""),
            f"{evidence_path}|{evidence_sha1}",
            "JSON",
        )
        row["derived_source_id"] = derived_source_id

        try:
            payload = json.loads(evidence_path.read_text(encoding="utf-8", errors="ignore"))
        except json.JSONDecodeError:
            row["import_status"] = "FAILED"
            append_reason(row, "invalid_json_response")
            continue

        normalized_rows = extract_rows_from_json_payload(payload, default_district=row.get("district", ""))
        if not normalized_rows:
            row["import_status"] = "FAILED"
            append_reason(row, "no_registry_rows_in_json_response")
            continue

        title = row.get("evidence_title", "") or f"{row.get('source_id', '')} JSON Response"
        materialize_derived_source(
            parent_source_row=source_row,
            derived_source_id=derived_source_id,
            title=title,
            rows=normalized_rows,
            source_url=row.get("evidence_url_final", "") or row.get("source_page", ""),
            source_type="registry_json_payload",
            access_method="json_response_import",
            page_role="json_payload",
            notes=f"json_sha1={evidence_sha1}",
        )
        row["import_status"] = "NORMALIZED"
        imported += 1

    update_manual_rows(manual_rows)
    print(f"import_json_registry complete: imported={imported}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
from pathlib import Path

from normalize_registry_evidence import (
    build_derived_source_id,
    extract_rows_from_html_tables,
    materialize_derived_source,
)
from pipeline_common import (
    DOCS_DIR,
    RAW_OFFICIAL_DIR,
    extract_title,
    file_sha1,
    get_manifest_row,
    read_csv_rows,
    save_text,
    schema_fieldnames,
    upsert_manifest_rows,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")
HTML_EVIDENCE_TYPES = {"html_snapshot", "html", "page_html"}


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return DOCS_DIR.parent / path


def update_manual_rows(updated_rows: list[dict[str, str]]) -> None:
    write_csv_rows(DOCS_DIR / "manual_capture_template.csv", OUTPUT_FIELDS, updated_rows)


def should_process(row: dict[str, str], cities: set[str], source_ids: set[str], manual_ids: set[str]) -> bool:
    if cities and row.get("city") not in cities:
        return False
    if source_ids and row.get("source_id") not in source_ids:
        return False
    if manual_ids and row.get("manual_id") not in manual_ids:
        return False
    evidence_type = row.get("evidence_type", "").strip().lower()
    if evidence_type not in HTML_EVIDENCE_TYPES and row.get("capture_status", "").upper() != "HTML_SAVED":
        return False
    return bool(row.get("evidence_file_path", "").strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Import HTML snapshot evidence into derived registry sources.")
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
            row["remark"] = f"{row.get('remark', '')} | missing_file:{evidence_path}".strip(" |")
            continue

        html_text = evidence_path.read_text(encoding="utf-8", errors="ignore")
        evidence_sha1 = file_sha1(evidence_path)
        derived_source_id = row.get("derived_source_id", "") or build_derived_source_id(
            row.get("source_id", ""),
            f"{evidence_path}|{evidence_sha1}",
            "HTML",
        )
        title = row.get("evidence_title", "") or extract_title(html_text) or evidence_path.stem
        normalized_rows = extract_rows_from_html_tables(html_text, default_district=row.get("district", ""))
        if normalized_rows:
            materialize_derived_source(
                parent_source_row=source_row,
                derived_source_id=derived_source_id,
                title=title,
                rows=normalized_rows,
                source_url=row.get("evidence_url_final", "") or row.get("source_page", ""),
                source_type="registry_snapshot_table",
                access_method="normalized_evidence",
                page_role="html_snapshot_table",
                notes=f"html_snapshot_sha1={evidence_sha1}",
            )
            row["import_status"] = "NORMALIZED"
        else:
            target_path = RAW_OFFICIAL_DIR / "registry" / f"{derived_source_id}.html"
            save_text(target_path, html_text)
            upsert_manifest_rows(
                [
                    {
                        "source_id": derived_source_id,
                        "city": source_row.get("city", ""),
                        "source_type": "registry_snapshot",
                        "source_name": title,
                        "official_or_platform": source_row.get("official_or_platform", "official-linked platform"),
                        "url_or_page_name": row.get("evidence_url_final", "") or row.get("source_page", ""),
                        "target_table": "nursery_registry_raw",
                        "target_fields": "institution name;address;phone;capacity;registry status",
                        "access_method": "html_snapshot_import",
                        "page_role": "html_snapshot",
                        "source_status": "confirmed_direct",
                        "parent_source_id": row.get("source_id", ""),
                        "record_granularity": "institution",
                        "priority": source_row.get("priority", "1"),
                        "update_date": "",
                        "last_verified_date": "",
                        "notes": f"raw_html_snapshot_sha1={evidence_sha1}",
                    }
                ]
            )
            row["import_status"] = "IMPORTED"

        row["derived_source_id"] = derived_source_id
        row["evidence_file_sha1"] = evidence_sha1
        imported += 1

    update_manual_rows(manual_rows)
    print(f"import_html_snapshots complete: imported={imported}")


if __name__ == "__main__":
    main()

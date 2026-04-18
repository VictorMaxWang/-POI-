from __future__ import annotations

from pipeline_common import CLEAN_DIR, DOCS_DIR, make_hash_id, read_csv_rows, schema_fieldnames, write_csv_rows


OUTPUT_FIELDS = schema_fieldnames("nursery_registry_raw.csv")
SKIP_STATUSES = {"TODO", "BLOCKED", "SKIPPED"}
LEGACY_EVIDENCE_TYPES = {"legacy_manual_row", "manual_row", "l6_manual_row"}


def should_merge(row: dict[str, str]) -> bool:
    has_minimum = bool(row.get("institution_name_raw", "").strip() and row.get("address_raw", "").strip())
    if not has_minimum:
        return False
    if row.get("capture_status", "").upper() in SKIP_STATUSES:
        return False

    evidence_type = row.get("evidence_type", "").strip().lower()
    if evidence_type in LEGACY_EVIDENCE_TYPES:
        return True

    # Backward compatibility for pre-evidence-template rows that only contain manual fields.
    if (
        not evidence_type
        and not row.get("evidence_file_path", "").strip()
        and not row.get("evidence_url_final", "").strip()
    ):
        return True
    return False


def build_raw_row(row: dict[str, str]) -> dict[str, str]:
    capture_status = row.get("capture_status", "").upper()
    parse_status = "manual_capture_verified" if capture_status == "VERIFIED" else "manual_capture_pending_review"
    manual_check_flag = "0" if capture_status == "VERIFIED" else "1"
    source_page = row.get("evidence_url_final", "") or row.get("source_page", "")
    return {
        "raw_row_id": make_hash_id(
            "regraw",
            row.get("source_id", ""),
            row.get("city", ""),
            row.get("district", ""),
            row.get("institution_name_raw", ""),
            row.get("address_raw", ""),
        ),
        "city": row.get("city", ""),
        "district": row.get("district", ""),
        "registry_batch_name": row.get("evidence_title", "") or row.get("task_batch", ""),
        "source_id": row.get("source_id", ""),
        "source_url": source_page,
        "source_publish_date": row.get("captured_at", ""),
        "institution_name_raw": row.get("institution_name_raw", ""),
        "address_raw": row.get("address_raw", ""),
        "operator_name_raw": row.get("operator_name_raw", ""),
        "institution_type_raw": "",
        "registry_status_raw": row.get("registry_status_raw", ""),
        "inclusive_flag_raw": row.get("inclusive_flag_raw", ""),
        "demo_flag_raw": row.get("demo_flag_raw", ""),
        "community_flag_raw": "",
        "phone_raw": row.get("phone_raw", ""),
        "capacity_raw": row.get("capacity_raw", ""),
        "fee_raw": "",
        "raw_text": row.get("remark", "") or row.get("evidence_title", ""),
        "parse_status": parse_status,
        "manual_check_flag": manual_check_flag,
    }


def main() -> None:
    existing_rows = read_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv")
    manual_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")

    merged_map = {row.get("raw_row_id", ""): {field: row.get(field, "") for field in OUTPUT_FIELDS} for row in existing_rows if row.get("raw_row_id")}
    added = 0
    for row in manual_rows:
        if not should_merge(row):
            continue
        raw_row = build_raw_row(row)
        raw_row_id = raw_row["raw_row_id"]
        if raw_row_id not in merged_map:
            merged_map[raw_row_id] = raw_row
            added += 1
            continue
        existing = merged_map[raw_row_id]
        for field in OUTPUT_FIELDS:
            if raw_row.get(field):
                existing[field] = raw_row[field]
        merged_map[raw_row_id] = existing

    merged_rows = sorted(
        merged_map.values(),
        key=lambda row: (
            row.get("city", ""),
            row.get("district", ""),
            row.get("source_id", ""),
            row.get("institution_name_raw", ""),
            row.get("address_raw", ""),
        ),
    )
    write_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv", OUTPUT_FIELDS, merged_rows)
    print(f"manual_capture merged: rows={len(merged_rows)} added={added}")


if __name__ == "__main__":
    main()

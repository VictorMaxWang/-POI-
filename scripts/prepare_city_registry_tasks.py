from __future__ import annotations

from city_registry_config import iter_manual_registry_tasks
from pipeline_common import DOCS_DIR, make_hash_id, read_csv_rows, schema_fieldnames, write_csv_rows


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")


def task_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        row.get("source_id", ""),
        row.get("city", ""),
        row.get("page_role", ""),
        row.get("source_page", ""),
    )


def should_replace_remark(existing_remark: str, task_remark: str) -> bool:
    if not task_remark:
        return False
    if not existing_remark:
        return True
    old_markers = ("逐机构补录", "逐条补录", "补机构名称", "先定位真公示页，再逐机构补录")
    return any(marker in existing_remark for marker in old_markers)


def merge_row_values(target: dict[str, str], incoming: dict[str, str], *, prefer_task_defaults: bool = False) -> dict[str, str]:
    merged = {field: target.get(field, "") for field in OUTPUT_FIELDS}
    for field in OUTPUT_FIELDS:
        incoming_value = incoming.get(field, "")
        if not incoming_value:
            continue
        if not merged.get(field):
            merged[field] = incoming_value
            continue
        if field == "remark" and should_replace_remark(merged.get(field, ""), incoming_value):
            merged[field] = incoming_value
        elif prefer_task_defaults and field in {
            "capture_mode",
            "access_channel",
            "public_access_confirmed",
            "import_status",
            "parser_hint",
            "evidence_type",
        }:
            merged[field] = incoming_value
    return merged


def infer_capture_defaults(page_role: str) -> tuple[str, str, str, str]:
    default_evidence_type = "html_snapshot"
    default_capture_mode = ""
    default_access_channel = "official_site"
    default_parser_hint = ""

    if "wechat" in page_role:
        default_evidence_type = "har"
        default_capture_mode = "browser_export"
        default_access_channel = "wechat_h5"
        default_parser_hint = "wechat_public_capture"
    elif "app" in page_role or "platform" in page_role:
        default_evidence_type = "har"
        default_capture_mode = "browser_export"
        default_access_channel = "app_webview"
        default_parser_hint = "public_h5_probe"
    elif "followdown" in page_role:
        default_evidence_type = "url_record"
        default_capture_mode = "url_record"
        default_access_channel = "official_site"
        default_parser_hint = "official_notice_probe"
    elif "notice" in page_role or "entry" in page_role:
        default_evidence_type = "html_snapshot"

    if "blocker" in page_role:
        default_evidence_type = "screenshot"
        default_capture_mode = "manual_note"

    return default_evidence_type, default_capture_mode, default_access_channel, default_parser_hint


def normalize_existing_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {field: row.get(field, "") for field in OUTPUT_FIELDS}
    evidence_type, capture_mode, access_channel, parser_hint = infer_capture_defaults(row.get("page_role", ""))
    normalized.setdefault("public_access_confirmed", "")
    if not normalized.get("evidence_type"):
        normalized["evidence_type"] = evidence_type
    if not normalized.get("capture_mode") and capture_mode:
        normalized["capture_mode"] = capture_mode
    if not normalized.get("access_channel") and access_channel:
        normalized["access_channel"] = access_channel
    if not normalized.get("public_access_confirmed"):
        normalized["public_access_confirmed"] = "1"
    if not normalized.get("import_status"):
        normalized["import_status"] = "PENDING"
    if not normalized.get("parser_hint") and parser_hint:
        normalized["parser_hint"] = parser_hint
    return normalized


def has_capture_progress(row: dict[str, str]) -> bool:
    progress_fields = [
        "evidence_file_path",
        "evidence_file_sha1",
        "captured_at",
        "capture_person",
        "derived_source_id",
        "screenshot_path",
        "institution_name_raw",
        "address_raw",
        "phone_raw",
    ]
    if any((row.get(field, "") or "").strip() for field in progress_fields):
        return True
    capture_status = (row.get("capture_status", "") or "").strip()
    if capture_status and capture_status not in {"", "TODO"}:
        return True
    import_status = (row.get("import_status", "") or "").strip()
    return import_status not in {"", "PENDING"}


def build_task_row(task: dict[str, str]) -> dict[str, str]:
    source_page = task.get("source_page", "")
    remark = task.get("remark", "")
    page_role = task.get("page_role", "")
    default_evidence_type, default_capture_mode, default_access_channel, default_parser_hint = infer_capture_defaults(page_role)

    return {
        "manual_id": make_hash_id("manual", task.get("source_id", ""), task.get("district", ""), source_page, remark),
        "task_batch": task.get("task_batch", f"{task.get('city', 'ALL')}_REGISTRY_BOOTSTRAP"),
        "capture_status": "TODO",
        "city": task.get("city", ""),
        "district": task.get("district", ""),
        "source_id": task.get("source_id", ""),
        "page_role": page_role,
        "parent_source_id": task.get("parent_source_id", ""),
        "source_page": source_page,
        "evidence_type": default_evidence_type,
        "evidence_title": task.get("evidence_title", ""),
        "evidence_url_final": "",
        "evidence_file_path": "",
        "evidence_file_sha1": "",
        "capture_mode": default_capture_mode,
        "access_channel": default_access_channel,
        "public_access_confirmed": "1",
        "captured_at": "",
        "institution_name_raw": "",
        "address_raw": "",
        "phone_raw": "",
        "operator_name_raw": "",
        "capacity_raw": "",
        "registry_status_raw": "",
        "inclusive_flag_raw": "",
        "demo_flag_raw": "",
        "capture_person": "",
        "import_status": "PENDING",
        "parser_hint": default_parser_hint,
        "derived_source_id": "",
        "screenshot_path": "",
        "remark": remark,
    }


def main() -> None:
    existing_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    merged_map: dict[tuple[str, str, str, str], dict[str, str]] = {}

    for row in existing_rows:
        if not has_capture_progress(row):
            continue
        key = task_key(row)
        existing = merged_map.get(key, {field: "" for field in OUTPUT_FIELDS})
        merged_map[key] = merge_row_values(existing, normalize_existing_row(row))

    for task in iter_manual_registry_tasks():
        row = build_task_row(task)
        key = task_key(row)
        existing = merged_map.get(key, {field: "" for field in OUTPUT_FIELDS})
        merged_map[key] = merge_row_values(existing, row, prefer_task_defaults=True)

    merged_rows = list(merged_map.values())

    merged_rows.sort(
        key=lambda row: (
            row.get("city", ""),
            row.get("task_batch", ""),
            row.get("source_id", ""),
            row.get("district", ""),
            row.get("page_role", ""),
            row.get("source_page", ""),
        )
    )
    write_csv_rows(DOCS_DIR / "manual_capture_template.csv", OUTPUT_FIELDS, merged_rows)
    print(f"manual_capture_template prepared: rows={len(merged_rows)}")


if __name__ == "__main__":
    main()

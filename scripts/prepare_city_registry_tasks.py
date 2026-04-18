from __future__ import annotations

from city_registry_config import iter_manual_registry_tasks
from pipeline_common import DOCS_DIR, make_hash_id, read_csv_rows, schema_fieldnames, write_csv_rows


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")


def task_key(row: dict[str, str]) -> tuple[str, str, str, str, str, str]:
    return (
        row.get("source_id", ""),
        row.get("city", ""),
        row.get("district", ""),
        row.get("page_role", ""),
        row.get("source_page", ""),
        row.get("remark", ""),
    )


def build_task_row(task: dict[str, str]) -> dict[str, str]:
    source_page = task.get("source_page", "")
    remark = task.get("remark", "")
    return {
        "manual_id": make_hash_id("manual", task.get("source_id", ""), task.get("district", ""), source_page, remark),
        "task_batch": task.get("task_batch", f"{task.get('city', 'ALL')}_REGISTRY_BOOTSTRAP"),
        "capture_status": "TODO",
        "city": task.get("city", ""),
        "district": task.get("district", ""),
        "source_id": task.get("source_id", ""),
        "page_role": task.get("page_role", ""),
        "parent_source_id": task.get("parent_source_id", ""),
        "source_page": source_page,
        "evidence_title": task.get("evidence_title", ""),
        "evidence_url_final": "",
        "institution_name_raw": "",
        "address_raw": "",
        "phone_raw": "",
        "operator_name_raw": "",
        "capacity_raw": "",
        "registry_status_raw": "",
        "inclusive_flag_raw": "",
        "demo_flag_raw": "",
        "capture_person": "",
        "capture_date": "",
        "screenshot_path": "",
        "remark": remark,
    }


def main() -> None:
    existing_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    merged_rows: list[dict[str, str]] = []
    seen = set()

    for row in existing_rows:
        key = task_key(row)
        if key in seen:
            continue
        seen.add(key)
        merged_rows.append({field: row.get(field, "") for field in OUTPUT_FIELDS})

    for task in iter_manual_registry_tasks():
        row = build_task_row(task)
        key = task_key(row)
        if key in seen:
            continue
        seen.add(key)
        merged_rows.append(row)

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

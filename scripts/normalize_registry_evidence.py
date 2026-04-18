from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from pipeline_common import (
    RAW_OFFICIAL_DIR,
    extract_tables_from_html,
    extract_title,
    make_hash_id,
    normalize_whitespace,
    render_registry_table_html,
    save_text,
    select_source_url,
    upsert_manifest_rows,
)


CANONICAL_FIELDS = [
    "district",
    "institution_name_raw",
    "address_raw",
    "phone_raw",
    "capacity_raw",
    "registry_status_raw",
    "institution_type_raw",
    "operator_name_raw",
    "inclusive_flag_raw",
    "demo_flag_raw",
]

FIELD_LABELS = {
    "district": "区县",
    "institution_name_raw": "机构名称",
    "address_raw": "地址",
    "phone_raw": "联系电话",
    "capacity_raw": "托位数",
    "registry_status_raw": "备案状态",
    "institution_type_raw": "机构类型",
    "operator_name_raw": "举办方",
    "inclusive_flag_raw": "普惠",
    "demo_flag_raw": "示范",
}

FIELD_ALIASES = {
    "district": ["区县", "地区", "行政区", "区域", "所在区"],
    "institution_name_raw": ["机构名称", "托育机构名称", "名称", "园名", "机构", "organname", "name"],
    "address_raw": ["地址", "详细地址", "机构地址", "所在地址", "address", "addr"],
    "phone_raw": ["电话", "联系电话", "咨询电话", "联系机构", "tel", "phone", "mobile"],
    "capacity_raw": ["托位", "托位数", "核定托位", "容量", "capacity"],
    "registry_status_raw": ["备案状态", "状态", "登记状态", "approve", "record_status"],
    "institution_type_raw": ["机构类型", "托育类型", "园所类型", "类型", "type", "service_scope"],
    "operator_name_raw": ["举办方", "举办单位", "运营方", "主办方", "operator", "organizer"],
    "inclusive_flag_raw": ["普惠", "inclusive"],
    "demo_flag_raw": ["示范", "demo"],
}


def match_field(name: str) -> str:
    normalized = normalize_whitespace(name).lower()
    best_field = ""
    best_alias_length = -1
    for field_name, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            alias_normalized = alias.lower()
            if alias_normalized in normalized and len(alias_normalized) > best_alias_length:
                best_field = field_name
                best_alias_length = len(alias_normalized)
    return best_field


def stringify_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join(filter(None, (stringify_value(item) for item in value)))
    if isinstance(value, dict):
        parts = [f"{key}:{stringify_value(item)}" for key, item in value.items() if stringify_value(item)]
        return " | ".join(parts)
    return normalize_whitespace(str(value))


def normalize_record(record: dict[str, object], default_district: str = "") -> dict[str, str]:
    normalized = {field_name: "" for field_name in CANONICAL_FIELDS}
    normalized["district"] = normalize_whitespace(default_district)
    for key, value in record.items():
        field_name = match_field(key)
        if not field_name:
            continue
        value_text = stringify_value(value)
        if value_text and not normalized[field_name]:
            normalized[field_name] = value_text
    return normalized


def is_plausible_registry_row(row: dict[str, str]) -> bool:
    name_value = normalize_whitespace(row.get("institution_name_raw", ""))
    if not name_value:
        return False
    if any(marker in name_value for marker in ("温馨提示", "六个关注", "建议", "注意事项")):
        return False
    if len(name_value) > 60 and not any(row.get(field, "") for field in ("address_raw", "phone_raw", "capacity_raw")):
        return False
    return True


def looks_like_registry_record(record: dict[str, object]) -> bool:
    matched_fields = {match_field(key) for key in record.keys()}
    return bool(
        "institution_name_raw" in matched_fields
        or (
            "address_raw" in matched_fields
            and ("phone_raw" in matched_fields or "capacity_raw" in matched_fields or "registry_status_raw" in matched_fields)
        )
    )


def iter_candidate_records(payload: object) -> Iterable[dict[str, object]]:
    if isinstance(payload, dict):
        if looks_like_registry_record(payload):
            yield payload
            return
        for value in payload.values():
            yield from iter_candidate_records(value)
        return
    if isinstance(payload, list):
        if payload and all(isinstance(item, dict) for item in payload) and any(looks_like_registry_record(item) for item in payload):
            for item in payload:
                if isinstance(item, dict):
                    yield item
            return
        for item in payload:
            yield from iter_candidate_records(item)


def extract_rows_from_json_payload(payload: object, default_district: str = "") -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in iter_candidate_records(payload):
        normalized = normalize_record(record, default_district=default_district)
        if is_plausible_registry_row(normalized):
            rows.append(normalized)
    return dedupe_rows(rows)


def select_best_header_row(table: list[list[str]]) -> tuple[dict[str, int], int]:
    best_indexes: dict[str, int] = {}
    best_start = 1
    for header_idx in range(min(2, len(table))):
        indexes: dict[str, int] = {}
        for cell_idx, cell in enumerate(table[header_idx]):
            field_name = match_field(cell)
            if field_name and field_name not in indexes:
                indexes[field_name] = cell_idx
        if len(indexes) > len(best_indexes):
            best_indexes = indexes
            best_start = header_idx + 1
    return best_indexes, best_start


def extract_rows_from_html_tables(html_text: str, default_district: str = "") -> list[dict[str, str]]:
    output_rows: list[dict[str, str]] = []
    for table in extract_tables_from_html(html_text):
        if len(table) < 2:
            continue
        indexes, start_row = select_best_header_row(table)
        if "institution_name_raw" not in indexes:
            continue
        for row in table[start_row:]:
            record = {field_name: row[idx] for field_name, idx in indexes.items() if idx < len(row)}
            normalized = normalize_record(record, default_district=default_district)
            if is_plausible_registry_row(normalized):
                output_rows.append(normalized)
    return dedupe_rows(output_rows)


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    output = []
    for row in rows:
        key = (
            row.get("district", ""),
            row.get("institution_name_raw", ""),
            row.get("address_raw", ""),
            row.get("phone_raw", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def rows_to_html(title: str, rows: list[dict[str, str]]) -> str:
    headers = [FIELD_LABELS[field_name] for field_name in CANONICAL_FIELDS]
    table_rows = [[row.get(field_name, "") for field_name in CANONICAL_FIELDS] for row in rows]
    return render_registry_table_html(headers, table_rows, title=title)


def build_derived_source_id(parent_source_id: str, seed: str, label: str) -> str:
    digest = make_hash_id("drv", parent_source_id, seed, label).split("_", 1)[1].upper()
    return f"{parent_source_id}_{label}_{digest[:8]}"


def materialize_derived_source(
    *,
    parent_source_row: dict[str, str],
    derived_source_id: str,
    title: str,
    rows: list[dict[str, str]],
    source_url: str,
    source_type: str,
    access_method: str,
    page_role: str,
    notes: str,
) -> Path:
    html_text = rows_to_html(title, rows)
    target_path = RAW_OFFICIAL_DIR / "registry" / f"{derived_source_id}.html"
    save_text(target_path, html_text)
    upsert_manifest_rows(
        [
            {
                "source_id": derived_source_id,
                "city": parent_source_row.get("city", ""),
                "source_type": source_type,
                "source_name": title,
                "official_or_platform": parent_source_row.get("official_or_platform", "official"),
                "url_or_page_name": source_url or select_source_url(parent_source_row),
                "target_table": parent_source_row.get("target_table", "nursery_registry_raw"),
                "target_fields": "institution name;address;phone;capacity;registry status;institution type",
                "access_method": access_method,
                "page_role": page_role,
                "source_status": "confirmed_direct",
                "parent_source_id": parent_source_row.get("source_id", ""),
                "record_granularity": "institution",
                "priority": parent_source_row.get("priority", "1"),
                "update_date": parent_source_row.get("update_date", ""),
                "last_verified_date": "",
                "notes": notes,
            }
        ]
    )
    return target_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize registry evidence into parser-ready HTML tables.")
    parser.add_argument("--source-html", default="", help="Path to an HTML file that should be normalized")
    parser.add_argument("--title", default="", help="Optional title for the normalized HTML")
    parser.add_argument("--output", default="", help="Optional output path for normalized HTML")
    args = parser.parse_args()

    if not args.source_html:
        raise SystemExit("normalize_registry_evidence requires --source-html")

    source_path = Path(args.source_html)
    html_text = source_path.read_text(encoding="utf-8", errors="ignore")
    rows = extract_rows_from_html_tables(html_text)
    if not rows:
        raise SystemExit("No registry-like rows detected in HTML evidence.")

    title = args.title or extract_title(html_text) or source_path.stem
    output_path = Path(args.output) if args.output else source_path.with_name(f"{source_path.stem}_normalized.html")
    save_text(output_path, rows_to_html(title, rows))
    print(f"normalized_registry_evidence rows={len(rows)} output={output_path}")


if __name__ == "__main__":
    main()

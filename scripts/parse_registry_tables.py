from __future__ import annotations

import argparse
import json
import re

from pipeline_common import (
    CLEAN_DIR,
    RAW_OFFICIAL_DIR,
    extract_publish_date,
    extract_tables_from_html,
    extract_title,
    load_html_for_source,
    load_manifest,
    make_hash_id,
    normalize_whitespace,
    read_csv_rows,
    schema_fieldnames,
    select_source_url,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("nursery_registry_raw.csv")

FIELD_KEYWORDS = {
    "district": ["区县", "县(市、区)", "县（市、区）", "地区", "所在区", "行政区"],
    "institution_name_raw": ["机构名称", "托育机构名称", "名称", "园名", "机构"],
    "address_raw": ["地址", "详细地址", "所在地址", "机构地址", "园所地址"],
    "operator_name_raw": ["举办方", "举办单位", "运营方", "主办方", "主办单位"],
    "institution_type_raw": ["机构类型", "托育类型", "园所类型"],
    "registry_status_raw": ["备案状态", "状态", "登记状态"],
    "inclusive_flag_raw": ["普惠"],
    "demo_flag_raw": ["示范"],
    "community_flag_raw": ["社区"],
    "phone_raw": ["电话", "联系电话", "咨询电话"],
    "capacity_raw": ["托位", "托位数", "核定托位", "托育位"],
    "fee_raw": ["收费", "价格", "收费标准", "月费"],
}


def detect_column_indexes(header_cells: list[str]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    normalized_headers = [normalize_whitespace(cell) for cell in header_cells]
    for idx, header in enumerate(normalized_headers):
        for field_name, keywords in FIELD_KEYWORDS.items():
            if field_name in indexes:
                continue
            if any(keyword in header for keyword in keywords):
                indexes[field_name] = idx
    return indexes


def decode_js_value(raw_value: str) -> str:
    raw_value = normalize_whitespace(raw_value)
    if raw_value in {"", "a", "null", "undefined"}:
        return ""
    if raw_value.startswith('"') and raw_value.endswith('"'):
        try:
            return normalize_whitespace(json.loads(raw_value))
        except json.JSONDecodeError:
            return normalize_whitespace(raw_value.strip('"'))
    return raw_value


def build_row(
    source_row: dict[str, str],
    publish_date: str,
    title: str,
    source_url: str,
    row_values: list[str],
    indexes: dict[str, int],
    row_index: int,
) -> dict[str, str]:
    def pick(field_name: str) -> str:
        idx = indexes.get(field_name)
        if idx is None or idx >= len(row_values):
            return ""
        return normalize_whitespace(row_values[idx])

    name_value = pick("institution_name_raw")
    address_value = pick("address_raw")
    return {
        "raw_row_id": make_hash_id("regraw", source_row["source_id"], row_index, name_value, address_value),
        "city": source_row.get("city", ""),
        "district": pick("district"),
        "registry_batch_name": title or source_row.get("source_name", ""),
        "source_id": source_row.get("source_id", ""),
        "source_url": source_url,
        "source_publish_date": publish_date or source_row.get("update_date", "")[:10],
        "institution_name_raw": name_value,
        "address_raw": address_value,
        "operator_name_raw": pick("operator_name_raw"),
        "institution_type_raw": pick("institution_type_raw"),
        "registry_status_raw": pick("registry_status_raw"),
        "inclusive_flag_raw": pick("inclusive_flag_raw"),
        "demo_flag_raw": pick("demo_flag_raw"),
        "community_flag_raw": pick("community_flag_raw"),
        "phone_raw": pick("phone_raw"),
        "capacity_raw": pick("capacity_raw"),
        "fee_raw": pick("fee_raw"),
        "raw_text": " | ".join(row_values),
        "parse_status": "parsed_table",
        "manual_check_flag": "1" if not name_value or not address_value else "0",
    }


def parse_source_table(source_row: dict[str, str], html_text: str) -> list[dict[str, str]]:
    if source_row.get("source_type") == "registry_entry":
        return []

    tables = extract_tables_from_html(html_text)
    output_rows: list[dict[str, str]] = []
    title = extract_title(html_text)
    publish_date = extract_publish_date(html_text)
    source_url = select_source_url(source_row)

    for table in tables:
        if len(table) < 2:
            continue
        header_candidates = [normalize_whitespace(" ".join(table[0]))]
        if len(table) > 1:
            header_candidates.append(normalize_whitespace(" ".join(table[1])))
        header_text = " ".join(header_candidates)
        if not any(keyword in header_text for keyword in ("机构", "名称", "托育")):
            continue

        header_indexes = detect_column_indexes(table[0])
        start_row = 1
        if "institution_name_raw" not in header_indexes and len(table) > 1:
            header_indexes = detect_column_indexes(table[1])
            start_row = 2
        if "institution_name_raw" not in header_indexes:
            continue
        if "address_raw" not in header_indexes and source_row.get("source_type") not in {"registry_list"}:
            continue

        for idx, row_values in enumerate(table[start_row:], start=start_row):
            if not row_values:
                continue
            parsed = build_row(source_row, publish_date, title, source_url, row_values, header_indexes, idx)
            if not parsed["institution_name_raw"]:
                continue
            output_rows.append(parsed)
    return output_rows


def parse_nantong_ssr_source(source_row: dict[str, str], html_text: str) -> list[dict[str, str]]:
    list_match = re.search(r"organList:\[(.*?)]\s*,total:", html_text, flags=re.S)
    if not list_match:
        return []

    list_body = list_match.group(1)
    pattern = re.compile(
        r'id:"(?P<id>[^"]+)"'
        r'.{0,800}?organname:"(?P<name>(?:\\.|[^"])*)"'
        r'.{0,4000}?address:(?P<address>"(?:\\.|[^"])*"|a)'
        r'.{0,1200}?tel:(?P<tel>"(?:\\.|[^"])*"|a)',
        flags=re.S,
    )

    source_url = select_source_url(source_row)
    publish_date = source_row.get("update_date", "")[:10]
    batch_name = source_row.get("source_name", "")
    output_rows: list[dict[str, str]] = []
    for idx, match in enumerate(pattern.finditer(list_body), start=1):
        name_value = decode_js_value(f'"{match.group("name")}"')
        address_value = decode_js_value(match.group("address"))
        phone_value = decode_js_value(match.group("tel"))
        if not name_value:
            continue
        raw_text_parts = [name_value]
        if address_value:
            raw_text_parts.append(address_value)
        if phone_value:
            raw_text_parts.append(phone_value)
        output_rows.append(
            {
                "raw_row_id": make_hash_id("regraw", source_row["source_id"], match.group("id"), name_value, address_value),
                "city": source_row.get("city", ""),
                "district": "",
                "registry_batch_name": batch_name,
                "source_id": source_row.get("source_id", ""),
                "source_url": source_url,
                "source_publish_date": publish_date,
                "institution_name_raw": name_value,
                "address_raw": address_value,
                "operator_name_raw": "",
                "institution_type_raw": "",
                "registry_status_raw": "",
                "inclusive_flag_raw": "",
                "demo_flag_raw": "",
                "community_flag_raw": "",
                "phone_raw": phone_value,
                "capacity_raw": "",
                "fee_raw": "",
                "raw_text": " | ".join(raw_text_parts),
                "parse_status": "parsed_ssr_list",
                "manual_check_flag": "1" if not address_value else "0",
            }
        )
    return output_rows


def parse_source(source_row: dict[str, str], html_text: str) -> list[dict[str, str]]:
    if source_row.get("source_id") == "NT_REG_ORGAN_SEARCH":
        return parse_nantong_ssr_source(source_row, html_text)
    return parse_source_table(source_row, html_text)


def preserved_manual_rows() -> list[dict[str, str]]:
    existing_rows = read_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv")
    return [
        row
        for row in existing_rows
        if row.get("parse_status", "").startswith("manual_capture_")
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse fetched registry pages into nursery_registry_raw.csv.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    args = parser.parse_args()

    manifest = [row for row in load_manifest() if row.get("target_table") == "nursery_registry_raw"]
    if args.city:
        manifest = [row for row in manifest if row.get("city") in set(args.city)]

    output_rows: list[dict[str, str]] = preserved_manual_rows()
    for source_row in manifest:
        html_text = load_html_for_source(source_row["source_id"], RAW_OFFICIAL_DIR / "registry")
        if not html_text:
            continue
        output_rows.extend(parse_source(source_row, html_text))

    deduped: list[dict[str, str]] = []
    seen = set()
    for row in output_rows:
        key = row["raw_row_id"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    write_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv", OUTPUT_FIELDS, deduped)
    print(f"registry_parse complete: rows={len(deduped)}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse

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
    schema_fieldnames,
    select_source_url,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("nursery_registry_raw.csv")


def detect_column_indexes(header_cells: list[str]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    for idx, header in enumerate(header_cells):
        if ("机构" in header or "名称" in header) and "institution_name_raw" not in indexes:
            indexes["institution_name_raw"] = idx
        if "地址" in header and "address_raw" not in indexes:
            indexes["address_raw"] = idx
        if ("举办" in header or "主办" in header or "运营" in header) and "operator_name_raw" not in indexes:
            indexes["operator_name_raw"] = idx
        if "类型" in header and "institution_type_raw" not in indexes:
            indexes["institution_type_raw"] = idx
        if ("备案" in header or "状态" in header) and "registry_status_raw" not in indexes:
            indexes["registry_status_raw"] = idx
        if "普惠" in header and "inclusive_flag_raw" not in indexes:
            indexes["inclusive_flag_raw"] = idx
        if "示范" in header and "demo_flag_raw" not in indexes:
            indexes["demo_flag_raw"] = idx
        if "社区" in header and "community_flag_raw" not in indexes:
            indexes["community_flag_raw"] = idx
        if "电话" in header and "phone_raw" not in indexes:
            indexes["phone_raw"] = idx
        if "托位" in header and "capacity_raw" not in indexes:
            indexes["capacity_raw"] = idx
        if ("收费" in header or "价格" in header) and "fee_raw" not in indexes:
            indexes["fee_raw"] = idx
    return indexes


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
        "district": "",
        "registry_batch_name": title,
        "source_id": source_row.get("source_id", ""),
        "source_url": source_url,
        "source_publish_date": publish_date,
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
        if "机构" not in header_text and "名称" not in header_text and "托育" not in header_text:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse fetched registry pages into nursery_registry_raw.csv.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    args = parser.parse_args()

    manifest = [row for row in load_manifest() if row.get("target_table") == "nursery_registry_raw"]
    if args.city:
        manifest = [row for row in manifest if row.get("city") in set(args.city)]

    output_rows: list[dict[str, str]] = []
    for source_row in manifest:
        html_text = load_html_for_source(source_row["source_id"], RAW_OFFICIAL_DIR / "registry")
        if not html_text:
            continue
        output_rows.extend(parse_source_table(source_row, html_text))

    deduped = []
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

from __future__ import annotations

from collections import Counter

from pipeline_common import CLEAN_DIR, LOGS_DIR, make_hash_id, read_csv_rows, schema_fieldnames, write_csv_rows


OUTPUT_FIELDS = schema_fieldnames("manual_review_list.csv")


def main() -> None:
    review_rows = []

    nursery_rows = read_csv_rows(CLEAN_DIR / "nursery_master.csv")
    geo_rows = read_csv_rows(CLEAN_DIR / "geo_result.csv")
    registry_rows = read_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv")
    text_tag_rows = read_csv_rows(LOGS_DIR.parent / "text" / "text_tag_suggest.csv")

    name_counter = Counter((row.get("city", ""), row.get("institution_name_std", "")) for row in nursery_rows if row.get("institution_name_std"))
    geo_map = {row.get("ref_id", ""): row for row in geo_rows if row.get("ref_id")}

    for row in nursery_rows:
        ref_id = row.get("nursery_id", "")
        name_key = (row.get("city", ""), row.get("institution_name_std", ""))
        if name_counter[name_key] > 1:
            review_rows.append(
                {
                    "review_id": make_hash_id("review", "dup_name", ref_id),
                    "review_type": "duplicate_name",
                    "ref_table": "nursery_master.csv",
                    "ref_id": ref_id,
                    "city": row.get("city", ""),
                    "district": row.get("district", ""),
                    "issue_summary": "同城同名机构需人工确认是否重复",
                    "issue_detail": row.get("institution_name_std", ""),
                    "source_id": row.get("source_latest_id", ""),
                    "manual_action": "核对地址、举办方和备案来源后确认是否合并。",
                    "status": "TODO",
                }
            )
        geo_row = geo_map.get(ref_id)
        if row.get("review_status") == "READY_FOR_GEOCODE" and not geo_row:
            review_rows.append(
                {
                    "review_id": make_hash_id("review", "missing_geo", ref_id),
                    "review_type": "missing_geo_result",
                    "ref_table": "nursery_master.csv",
                    "ref_id": ref_id,
                    "city": row.get("city", ""),
                    "district": row.get("district", ""),
                    "issue_summary": "待地理编码记录尚无结果",
                    "issue_detail": row.get("address_std", ""),
                    "source_id": row.get("source_latest_id", ""),
                    "manual_action": "检查 AMap key、地址质量和 geocode 执行日志。",
                    "status": "TODO",
                }
            )

    for row in geo_rows:
        if row.get("manual_check_flag") == "1":
            review_rows.append(
                {
                    "review_id": make_hash_id("review", "geo_manual", row.get("geo_id", "")),
                    "review_type": "geo_manual_check",
                    "ref_table": "geo_result.csv",
                    "ref_id": row.get("geo_id", ""),
                    "city": row.get("city", ""),
                    "district": row.get("district", ""),
                    "issue_summary": "地理编码结果需人工核对",
                    "issue_detail": row.get("formatted_address", "") or row.get("address_input", ""),
                    "source_id": row.get("source_id", ""),
                    "manual_action": "核对坐标、区县匹配和 geocode level。",
                    "status": "TODO",
                }
            )

    for row in registry_rows:
        if row.get("manual_check_flag") == "1":
            review_rows.append(
                {
                    "review_id": make_hash_id("review", "registry_manual", row.get("raw_row_id", "")),
                    "review_type": "registry_manual_check",
                    "ref_table": "nursery_registry_raw.csv",
                    "ref_id": row.get("raw_row_id", ""),
                    "city": row.get("city", ""),
                    "district": row.get("district", ""),
                    "issue_summary": "机构原始记录字段不完整",
                    "issue_detail": row.get("raw_text", ""),
                    "source_id": row.get("source_id", ""),
                    "manual_action": "补核机构名称、地址和字段映射。",
                    "status": "TODO",
                }
            )

    for row in text_tag_rows:
        if row.get("ai_tag") and not row.get("human_tag_final"):
            review_rows.append(
                {
                    "review_id": make_hash_id("review", "text_tag", row.get("text_id", "")),
                    "review_type": "text_tag_pending",
                    "ref_table": "text_tag_suggest.csv",
                    "ref_id": row.get("text_id", ""),
                    "city": "",
                    "district": "",
                    "issue_summary": "规则标签待人工确认",
                    "issue_detail": row.get("rule_hit_detail", ""),
                    "source_id": "INST_TEXT_OFFICIAL",
                    "manual_action": "核对 ai_tag 命中依据并填写 human_tag_final。",
                    "status": "TODO",
                }
            )

    seen = set()
    deduped = []
    for row in review_rows:
        key = row["review_id"]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    write_csv_rows(LOGS_DIR / "manual_review_list.csv", OUTPUT_FIELDS, deduped)
    print(f"manual_review_list generated: rows={len(deduped)}")


if __name__ == "__main__":
    main()

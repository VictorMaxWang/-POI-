from __future__ import annotations

from collections import defaultdict

from pipeline_common import (
    CLEAN_DIR,
    load_manifest,
    make_hash_id,
    normalize_address,
    normalize_flag,
    normalize_name,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("nursery_master.csv")


def derive_institution_form(row_group: list[dict[str, str]]) -> str:
    text = " ".join(
        f"{row.get('institution_type_raw', '')} {row.get('raw_text', '')}"
        for row in row_group
    )
    if "幼儿园" in text or "托班" in text:
        return "幼儿园托班/托幼一体"
    if "社区" in text:
        return "社区托育点"
    if "托儿所" in text:
        return "托儿所"
    if "托育" in text:
        return "托育机构"
    return ""


def derive_flag(row_group: list[dict[str, str]], field_name: str, keywords: list[str]) -> str:
    for row in row_group:
        raw_value = normalize_flag(row.get(field_name, ""))
        if raw_value == "1":
            return "1"
    corpus = " ".join(row.get("raw_text", "") for row in row_group)
    for keyword in keywords:
        if keyword in corpus:
            return "1"
    return ""


def main() -> None:
    registry_rows = read_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv")
    output_rows = []
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)

    for row in registry_rows:
        name_std = normalize_name(row.get("institution_name_raw", ""))
        address_std = normalize_address(row.get("address_raw", ""))
        if not name_std:
            continue
        key = "||".join(
            [
                row.get("city", ""),
                row.get("district", ""),
                name_std,
                address_std or "__NO_ADDRESS__",
            ]
        )
        grouped[key].append(row)

    for key, group_rows in grouped.items():
        sample = group_rows[0]
        name_std = normalize_name(sample.get("institution_name_raw", ""))
        address_std = normalize_address(sample.get("address_raw", ""))
        aliases = sorted(
            {normalize_name(row.get("institution_name_raw", "")) for row in group_rows if normalize_name(row.get("institution_name_raw", "")) and normalize_name(row.get("institution_name_raw", "")) != name_std}
        )
        operator_names = sorted(
            {normalize_name(row.get("operator_name_raw", "")) for row in group_rows if normalize_name(row.get("operator_name_raw", ""))}
        )
        source_ids = [row.get("source_id", "") for row in group_rows if row.get("source_id")]
        publish_pairs = sorted(
            (row.get("source_publish_date", ""), row.get("source_id", ""))
            for row in group_rows
        )
        source_first_id = publish_pairs[0][1] if publish_pairs else (source_ids[0] if source_ids else "")
        source_latest_id = publish_pairs[-1][1] if publish_pairs else (source_ids[-1] if source_ids else "")

        ready_for_geocode = bool(address_std and name_std)
        output_rows.append(
            {
                "nursery_id": make_hash_id("nursery", sample.get("city", ""), sample.get("district", ""), name_std, address_std),
                "city": sample.get("city", ""),
                "district": sample.get("district", ""),
                "institution_name_std": name_std,
                "institution_name_aliases": "|".join(aliases),
                "institution_form": derive_institution_form(group_rows),
                "operator_name_std": "|".join(operator_names),
                "address_std": address_std,
                "source_first_id": source_first_id,
                "source_latest_id": source_latest_id,
                "inclusive_flag": derive_flag(group_rows, "inclusive_flag_raw", ["普惠"]),
                "community_embedded_flag": derive_flag(group_rows, "community_flag_raw", ["社区嵌入", "社区"]),
                "kindergarten_integrated_flag": derive_flag(group_rows, "institution_type_raw", ["托幼一体", "托班", "幼儿园"]),
                "medical_integration_flag": derive_flag(group_rows, "raw_text", ["医育结合", "儿保", "妇幼"]),
                "delayed_tempcare_flag": derive_flag(group_rows, "raw_text", ["延时", "临托", "小时托"]),
                "price_transparent_flag": derive_flag(group_rows, "fee_raw", ["收费", "价格公示"]),
                "teacher_emphasis_flag": derive_flag(group_rows, "raw_text", ["师资", "育婴师", "保育员"]),
                "chain_brand_flag": derive_flag(group_rows, "raw_text", ["连锁", "品牌", "直营"]),
                "registry_evidence_count": len(group_rows),
                "geo_status": "READY_FOR_GEOCODE" if ready_for_geocode else "BLOCKED_ADDRESS_MISSING",
                "text_status": "PENDING",
                "review_status": "READY_FOR_GEOCODE" if ready_for_geocode else "REVIEW_REQUIRED",
            }
        )

    write_csv_rows(CLEAN_DIR / "nursery_master.csv", OUTPUT_FIELDS, output_rows)
    print(f"nursery_master complete: rows={len(output_rows)}")


if __name__ == "__main__":
    main()

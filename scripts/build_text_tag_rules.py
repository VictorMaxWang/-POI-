from __future__ import annotations

import re
from pathlib import Path

from pipeline_common import (
    RAW_OFFICIAL_DIR,
    TEXT_DIR,
    extract_title,
    extract_visible_text,
    label_text,
    load_manifest,
    make_hash_id,
    now_ts,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)


TEXT_RAW_FIELDS = schema_fieldnames("text_raw.csv")
TEXT_TAG_FIELDS = schema_fieldnames("text_tag_suggest.csv")


def detect_phone(text_value: str) -> str:
    match = re.search(r"((?:0\d{2,3}-)?\d{7,8}|1\d{10})", text_value)
    return match.group(1) if match else ""


def detect_price(text_value: str) -> str:
    match = re.search(r"(\d{2,5}\s*元(?:/月|/人/月|/期)?)", text_value)
    return match.group(1) if match else ""


def nursery_meta_map() -> dict[str, dict[str, str]]:
    rows = read_csv_rows(Path(TEXT_DIR.parent) / "clean" / "nursery_master.csv")
    return {row.get("nursery_id", ""): row for row in rows if row.get("nursery_id")}


def find_source_url(source_id: str) -> str:
    for row in load_manifest():
        if row.get("source_id") == source_id:
            return row.get("url_or_page_name", "")
    return ""


def main() -> None:
    text_pages_dir = RAW_OFFICIAL_DIR / "text_pages"
    text_pages_dir.mkdir(parents=True, exist_ok=True)
    nursery_meta = nursery_meta_map()

    text_raw_rows = []
    tag_rows = []
    for path in sorted(text_pages_dir.glob("*")):
        if path.suffix.lower() not in {".html", ".htm", ".txt"}:
            continue
        stem_parts = path.stem.split("__")
        nursery_id = stem_parts[0] if stem_parts else ""
        source_id = stem_parts[1] if len(stem_parts) >= 2 else "INST_TEXT_OFFICIAL"
        meta = nursery_meta.get(nursery_id, {})
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
        if path.suffix.lower() in {".html", ".htm"}:
            page_title = extract_title(raw_text)
            body_text = extract_visible_text(raw_text)
        else:
            page_title = path.stem
            body_text = raw_text.strip()
        if not body_text:
            continue

        text_id = make_hash_id("text", nursery_id, source_id, path.name)
        text_raw_rows.append(
            {
                "text_id": text_id,
                "nursery_id": nursery_id,
                "city": meta.get("city", ""),
                "district": meta.get("district", ""),
                "source_id": source_id,
                "source_url": find_source_url(source_id),
                "page_title": page_title,
                "body_text": body_text,
                "public_phone_if_any": detect_phone(body_text),
                "public_price_if_any": detect_price(body_text),
                "capture_time": now_ts(),
                "manual_check_flag": "0",
            }
        )

        ai_tag, rule_hit_detail = label_text(body_text)
        tag_rows.append(
            {
                "text_id": text_id,
                "nursery_id": nursery_id,
                "ai_tag": ai_tag,
                "rule_hit_detail": rule_hit_detail,
                "human_tag_final": "",
                "reviewer": "",
                "review_date": "",
            }
        )

    write_csv_rows(TEXT_DIR / "text_raw.csv", TEXT_RAW_FIELDS, text_raw_rows)
    write_csv_rows(TEXT_DIR / "text_tag_suggest.csv", TEXT_TAG_FIELDS, tag_rows)
    print(f"text_tag_rules complete: text_rows={len(text_raw_rows)} tag_rows={len(tag_rows)}")


if __name__ == "__main__":
    main()

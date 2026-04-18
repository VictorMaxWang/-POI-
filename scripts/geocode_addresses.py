from __future__ import annotations

import argparse
import json
import os
import urllib.parse
from pathlib import Path

from pipeline_common import (
    CLEAN_DIR,
    RAW_API_DIR,
    append_blocker,
    fetch_json,
    load_manifest,
    make_hash_id,
    now_ts,
    normalize_whitespace,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("geo_result.csv")


def find_source_row(source_id: str) -> dict[str, str]:
    for row in load_manifest():
        if row.get("source_id") == source_id:
            return row
    return {
        "source_id": source_id,
        "city": "ALL",
        "url_or_page_name": "",
        "target_table": "geo_result",
    }


def get_api_key(cli_value: str | None) -> str:
    return cli_value or os.environ.get("AMAP_WEB_API_KEY", "") or os.environ.get("AMAP_KEY", "")


def cache_path(nursery_id: str) -> Path:
    return RAW_API_DIR / "amap_geocode" / f"{nursery_id}.json"


def build_request_url(api_key: str, address: str, city: str) -> str:
    params = {
        "key": api_key,
        "address": address,
        "city": city,
    }
    return "https://restapi.amap.com/v3/geocode/geo?" + urllib.parse.urlencode(params)


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode nursery addresses with AMap.")
    parser.add_argument("--api-key", default="", help="Override AMap API key")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of records")
    parser.add_argument("--force", action="store_true", help="Ignore cache and refetch")
    args = parser.parse_args()

    api_key = get_api_key(args.api_key)
    master_rows = read_csv_rows(CLEAN_DIR / "nursery_master.csv")
    if args.limit:
        master_rows = master_rows[: args.limit]

    if not api_key:
        append_blocker(
            "geocode",
            find_source_row("AMAP_GEOCODE_DOC"),
            "missing_api_key",
            "设置环境变量 AMAP_WEB_API_KEY 后再运行 geocode_addresses.py。",
        )
        write_csv_rows(CLEAN_DIR / "geo_result.csv", OUTPUT_FIELDS, [])
        print("geocode complete: rows=0 blocked=missing_api_key")
        return

    results = []
    for row in master_rows:
        if row.get("review_status") != "READY_FOR_GEOCODE":
            continue
        address_std = normalize_whitespace(row.get("address_std", ""))
        if not address_std:
            continue
        nursery_id = row.get("nursery_id", "")
        target_cache = cache_path(nursery_id)
        payload = {}
        if target_cache.exists() and not args.force:
            payload = json.loads(target_cache.read_text(encoding="utf-8"))
        else:
            request_url = build_request_url(api_key, address_std, row.get("city", ""))
            fetch_result = fetch_json(request_url, timeout=30)
            payload = fetch_result.get("json", {}) if isinstance(fetch_result.get("json", {}), dict) else {}
            target_cache.parent.mkdir(parents=True, exist_ok=True)
            target_cache.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        geocodes = payload.get("geocodes", []) if isinstance(payload, dict) else []
        geocode = geocodes[0] if geocodes else {}
        location = normalize_whitespace(geocode.get("location", "")) if isinstance(geocode, dict) else ""
        lng_value, lat_value = ("", "")
        if "," in location:
            lng_value, lat_value = [part.strip() for part in location.split(",", 1)]
        district_name = geocode.get("district", "") if isinstance(geocode, dict) else ""
        level = geocode.get("level", "") if isinstance(geocode, dict) else ""
        manual_check_flag = "1" if not location or (row.get("district") and district_name and row.get("district") != district_name) else "0"

        results.append(
            {
                "geo_id": make_hash_id("geo", nursery_id, address_std),
                "object_type": "nursery",
                "ref_id": nursery_id,
                "city": row.get("city", ""),
                "district": row.get("district", ""),
                "address_input": address_std,
                "address_std": address_std,
                "geocode_source": "amap_geocode_v3",
                "formatted_address": geocode.get("formatted_address", "") if isinstance(geocode, dict) else "",
                "province": geocode.get("province", "") if isinstance(geocode, dict) else "",
                "city_name": geocode.get("city", "") if isinstance(geocode, dict) else "",
                "district_name": district_name,
                "township": geocode.get("township", "") if isinstance(geocode, dict) else "",
                "adcode": geocode.get("adcode", "") if isinstance(geocode, dict) else "",
                "lng_gcj02": lng_value,
                "lat_gcj02": lat_value,
                "geocode_level": level or ("NO_RESULT" if not geocodes else ""),
                "source_id": row.get("source_latest_id") or row.get("source_first_id", ""),
                "geocode_time": now_ts(),
                "manual_check_flag": manual_check_flag,
            }
        )

    write_csv_rows(CLEAN_DIR / "geo_result.csv", OUTPUT_FIELDS, results)
    print(f"geocode complete: rows={len(results)}")


if __name__ == "__main__":
    main()

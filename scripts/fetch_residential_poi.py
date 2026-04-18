from __future__ import annotations

import argparse
import json
import os
import urllib.parse

from pipeline_common import (
    CLEAN_DIR,
    RAW_API_DIR,
    append_blocker,
    fetch_json,
    load_manifest,
    make_hash_id,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)
from pipeline_schema import DEFAULT_CITY_ADCODES, DEFAULT_RESIDENTIAL_KEYWORDS


OUTPUT_FIELDS = schema_fieldnames("poi_residential.csv")


def find_source_row(source_id: str) -> dict[str, str]:
    for row in load_manifest():
        if row.get("source_id") == source_id:
            return row
    return {"source_id": source_id, "city": "ALL", "url_or_page_name": "", "target_table": "poi_residential"}


def get_api_key(cli_value: str | None) -> str:
    return cli_value or os.environ.get("AMAP_WEB_API_KEY", "") or os.environ.get("AMAP_KEY", "")


def build_scopes() -> list[dict[str, str]]:
    population_rows = read_csv_rows(CLEAN_DIR / "population_city_district.csv")
    nursery_rows = read_csv_rows(CLEAN_DIR / "nursery_master.csv")
    scopes = []
    seen = set()
    population_cities = set()

    def add_scope(city: str, district: str) -> None:
        key = (city, district)
        if not city or key in seen:
            return
        seen.add(key)
        scopes.append({"city": city, "district": district, "adcode": DEFAULT_CITY_ADCODES.get(city, "")})

    for row in population_rows:
        city = row.get("city", "")
        district = row.get("district", "")
        population_cities.add(city)
        add_scope(city, district)

    for row in nursery_rows:
        city = row.get("city", "")
        if city in population_cities:
            continue
        add_scope(city, row.get("district", ""))

    if not scopes:
        for city, adcode in DEFAULT_CITY_ADCODES.items():
            scopes.append({"city": city, "district": "", "adcode": adcode})
    return scopes


def build_url(api_key: str, city: str, district: str, keyword: str, page: int) -> str:
    keywords = f"{district}{keyword}" if district else keyword
    params = {
        "key": api_key,
        "keywords": keywords,
        "city": city,
        "citylimit": "true",
        "offset": "20",
        "page": str(page),
        "extensions": "base",
    }
    return "https://restapi.amap.com/v3/place/text?" + urllib.parse.urlencode(params)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch residential POI from AMap.")
    parser.add_argument("--api-key", default="", help="Override AMap API key")
    parser.add_argument("--max-pages", type=int, default=3, help="Max pages per keyword")
    args = parser.parse_args()

    api_key = get_api_key(args.api_key)
    if not api_key:
        append_blocker(
            "poi_residential",
            find_source_row("AMAP_POI_DOC"),
            "missing_api_key",
            "设置环境变量 AMAP_WEB_API_KEY 后再运行 fetch_residential_poi.py。",
        )
        write_csv_rows(CLEAN_DIR / "poi_residential.csv", OUTPUT_FIELDS, [])
        print("poi_residential complete: rows=0 blocked=missing_api_key")
        return

    output_rows = []
    scopes = build_scopes()
    for scope in scopes:
        for keyword in DEFAULT_RESIDENTIAL_KEYWORDS:
            query_batch_id = make_hash_id("poiquery", "residential", scope["city"], scope["district"], keyword)
            for page in range(1, args.max_pages + 1):
                request_url = build_url(api_key, scope["city"], scope["district"], keyword, page)
                fetch_result = fetch_json(request_url, timeout=30)
                payload = fetch_result.get("json", {}) if isinstance(fetch_result.get("json", {}), dict) else {}
                raw_path = RAW_API_DIR / "amap_poi_residential" / f"{query_batch_id}_p{page}.json"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                pois = payload.get("pois", []) if isinstance(payload, dict) else []
                if not pois:
                    break
                for poi in pois:
                    poi_id = poi.get("id", "")
                    duplicate_key = poi_id or f"{poi.get('name', '')}||{poi.get('address', '')}"
                    output_rows.append(
                        {
                            "poi_row_id": make_hash_id("poi", "residential", duplicate_key, query_batch_id),
                            "poi_group": "residential",
                            "city": scope["city"],
                            "district": scope["district"],
                            "query_keyword": keyword,
                            "query_type_code": "",
                            "adcode": scope["adcode"],
                            "search_mode": "city_keyword_prefix",
                            "source_id": "AMAP_POI_DOC",
                            "poi_id": poi_id,
                            "poi_name": poi.get("name", ""),
                            "poi_type": poi.get("type", ""),
                            "poi_typecode": poi.get("typecode", ""),
                            "address": poi.get("address", ""),
                            "location_gcj02": poi.get("location", ""),
                            "tel": poi.get("tel", ""),
                            "parent_id": poi.get("parent", ""),
                            "business_area": poi.get("business_area", ""),
                            "query_batch_id": query_batch_id,
                            "duplicate_flag": "0",
                            "manual_check_flag": "1" if not poi.get("location") else "0",
                        }
                    )
                if len(pois) < 20:
                    break

    seen = set()
    for row in output_rows:
        dedupe_key = row["poi_id"] or f"{row['poi_name']}||{row['address']}"
        if dedupe_key in seen:
            row["duplicate_flag"] = "1"
        else:
            seen.add(dedupe_key)
    write_csv_rows(CLEAN_DIR / "poi_residential.csv", OUTPUT_FIELDS, output_rows)
    print(f"poi_residential complete: rows={len(output_rows)}")


if __name__ == "__main__":
    main()

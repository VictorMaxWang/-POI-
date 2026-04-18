from __future__ import annotations

import argparse
import json
import os
import re
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
    normalize_address,
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


def attempt_log_path(nursery_id: str) -> Path:
    return RAW_API_DIR / "amap_geocode" / f"{nursery_id}_attempts.json"


def build_request_url(api_key: str, address: str, city: str) -> str:
    params = {
        "key": api_key,
        "address": address,
        "city": city,
    }
    return "https://restapi.amap.com/v3/geocode/geo?" + urllib.parse.urlencode(params)


def safe_load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_attempt_metadata(nursery_id: str) -> tuple[str, bool]:
    metadata = safe_load_json(attempt_log_path(nursery_id))
    selected_address = normalize_whitespace(metadata.get("selected_address", "")) if metadata else ""
    used_fallback = bool(metadata.get("used_fallback")) if metadata else False
    return selected_address, used_fallback


def normalize_retry_address(address: str) -> str:
    normalized = normalize_address(address)
    replacements = {
        "（": "(",
        "）": ")",
        "【": "[",
        "】": "]",
        "，": ",",
        "、": ",",
        "；": ";",
        "－": "-",
        "—": "-",
    }
    for before, after in replacements.items():
        normalized = normalized.replace(before, after)
    normalized = re.sub(r"\s*([,;()\[\]-])\s*", r"\1", normalized)
    return normalized.replace(" ", "")


def ensure_city_prefix(address: str, city: str) -> str:
    if not city or address.startswith(city) or address.startswith(f"{city}市"):
        return address
    return f"{city}市{address}"


def strip_subaddress(address: str) -> str:
    trimmed = address
    strip_patterns = [
        r"(.*?\d+号)(?:[A-Za-z]?\d+(?:\.\d+)?(?:-\d+)?(?:室|层|楼|栋|幢|单元|商铺|铺))+$",
        r"(.*?(?:小区|花园|华庭|新村|公寓|广场|大厦|中心|商业楼|商业街|产业园|综合楼|综合体|未来城|理想城|河城|悦府|水岸|国际))(?:.*)$",
        r"(.*?\d+号)(?:[A-Za-z]楼.*)$",
        r"(.*?\d+号)(?:-\d+.*)$",
    ]
    for pattern in strip_patterns:
        match = re.match(pattern, trimmed)
        if match:
            candidate = normalize_whitespace(match.group(1))
            if candidate and candidate != trimmed:
                return candidate
    trimmed = re.sub(r"(?:\d+(?:\.\d+)?(?:-\d+)?室)$", "", trimmed)
    trimmed = re.sub(r"(?:[一二三四五六七八九十]+楼)$", "", trimmed)
    trimmed = re.sub(r"(?:\d+号楼\d*)$", "", trimmed)
    trimmed = normalize_whitespace(trimmed)
    return trimmed or address


def remove_institution_name(address: str, institution_name: str) -> str:
    if not institution_name or institution_name not in address:
        return address
    candidate = normalize_whitespace(address.replace(institution_name, "", 1))
    if candidate and candidate != address and re.search(r"(路|街|大道|巷|号)", candidate):
        return candidate
    return address


def build_address_candidates(row: dict[str, str], address_std: str) -> list[str]:
    candidates: list[str] = []

    def add(candidate: str) -> None:
        value = normalize_whitespace(candidate)
        if value and value not in candidates:
            candidates.append(value)

    normalized = normalize_retry_address(address_std)
    add(address_std)
    add(normalized)
    add(ensure_city_prefix(normalized, row.get("city", "")))
    stripped = strip_subaddress(normalized)
    add(stripped)
    add(ensure_city_prefix(stripped, row.get("city", "")))
    add(remove_institution_name(normalized, row.get("institution_name_std", "")))
    add(
        ensure_city_prefix(
            remove_institution_name(stripped, row.get("institution_name_std", "")),
            row.get("city", ""),
        )
    )
    return candidates


def payload_has_geocode(payload: dict[str, object]) -> bool:
    geocodes = payload.get("geocodes", []) if isinstance(payload, dict) else []
    return bool(geocodes)


def fetch_with_fallbacks(
    api_key: str,
    row: dict[str, str],
    address_std: str,
    *,
    force: bool,
) -> tuple[dict[str, object], str, bool]:
    nursery_id = row.get("nursery_id", "")
    target_cache = cache_path(nursery_id)
    log_path = attempt_log_path(nursery_id)
    cached_payload = safe_load_json(target_cache) if target_cache.exists() else {}

    if cached_payload and payload_has_geocode(cached_payload) and not force:
        selected_address, used_fallback = read_attempt_metadata(nursery_id)
        return cached_payload, selected_address or address_std, used_fallback

    candidates = build_address_candidates(row, address_std)
    attempts = []
    chosen_payload = cached_payload if isinstance(cached_payload, dict) else {}
    chosen_address = address_std
    used_fallback = False

    for index, candidate in enumerate(candidates):
        request_url = build_request_url(api_key, candidate, row.get("city", ""))
        fetch_result = fetch_json(request_url, timeout=30)
        payload = fetch_result.get("json", {}) if isinstance(fetch_result.get("json", {}), dict) else {}
        geocodes = payload.get("geocodes", []) if isinstance(payload, dict) else []
        attempts.append(
            {
                "attempt_index": index + 1,
                "address": candidate,
                "request_url": request_url,
                "status": payload.get("status", ""),
                "infocode": payload.get("infocode", ""),
                "count": len(geocodes),
            }
        )
        chosen_payload = payload
        chosen_address = candidate
        used_fallback = candidate != address_std
        if geocodes:
            break

    target_cache.parent.mkdir(parents=True, exist_ok=True)
    target_cache.write_text(json.dumps(chosen_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log_path.write_text(
        json.dumps(
            {
                "nursery_id": nursery_id,
                "city": row.get("city", ""),
                "district": row.get("district", ""),
                "institution_name_std": row.get("institution_name_std", ""),
                "address_std": address_std,
                "cached_payload_reused": False,
                "attempts": attempts,
                "selected_address": chosen_address,
                "selected_count": len(chosen_payload.get("geocodes", [])) if isinstance(chosen_payload, dict) else 0,
                "used_fallback": used_fallback,
                "updated_at": now_ts(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return chosen_payload, chosen_address, used_fallback


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
        payload, matched_address, used_fallback = fetch_with_fallbacks(
            api_key,
            row,
            address_std,
            force=args.force,
        )

        geocodes = payload.get("geocodes", []) if isinstance(payload, dict) else []
        geocode = geocodes[0] if geocodes else {}
        location = normalize_whitespace(geocode.get("location", "")) if isinstance(geocode, dict) else ""
        lng_value, lat_value = ("", "")
        if "," in location:
            lng_value, lat_value = [part.strip() for part in location.split(",", 1)]
        district_name = geocode.get("district", "") if isinstance(geocode, dict) else ""
        level = geocode.get("level", "") if isinstance(geocode, dict) else ""
        manual_check_flag = (
            "1"
            if used_fallback or not location or (row.get("district") and district_name and row.get("district") != district_name)
            else "0"
        )

        results.append(
            {
                "geo_id": make_hash_id("geo", nursery_id, address_std),
                "object_type": "nursery",
                "ref_id": nursery_id,
                "city": row.get("city", ""),
                "district": row.get("district", ""),
                "address_input": matched_address,
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

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from pipeline_common import (
    CLEAN_DIR,
    PROJECT_ROOT,
    append_csv_rows,
    make_hash_id,
    now_ts,
    read_csv_rows,
    write_csv_rows,
)


OUTPUT_DIR = PROJECT_ROOT / "output" / "accessibility_mvp"
RAW_OD_DIR = PROJECT_ROOT / "raw_api" / "amap_od"

OD_FIELDS = [
    "od_row_id",
    "city",
    "demand_poi_row_id",
    "demand_poi_id",
    "demand_name",
    "demand_district",
    "origin_lng",
    "origin_lat",
    "nursery_id",
    "nursery_name",
    "nursery_district",
    "dest_lng",
    "dest_lat",
    "euclid_distance_m",
    "euclid_rank",
    "travel_mode",
    "threshold_min",
    "walk_time_min",
    "walk_distance_m",
    "od_status",
    "request_id",
]

EXCLUDED_FIELDS = [
    "record_id",
    "city",
    "nursery_id",
    "institution_name_std",
    "address_std",
    "exclude_reason",
    "geo_status",
    "review_status",
    "geocode_level",
    "lng_gcj02",
    "lat_gcj02",
]

REQUEST_LOG_FIELDS = [
    "request_id",
    "request_time",
    "city",
    "demand_poi_row_id",
    "demand_poi_id",
    "demand_name",
    "candidate_count",
    "travel_mode",
    "request_url",
    "api_status",
    "infocode",
    "result_count",
    "success_count",
    "failure_count",
    "http_ok",
]


def get_api_key(cli_value: str | None) -> str:
    return cli_value or os.environ.get("AMAP_WEB_API_KEY", "") or os.environ.get("AMAP_KEY", "")


def parse_point(value: str) -> tuple[str, str]:
    text_value = str(value or "").strip().strip('"')
    if "," not in text_value:
        return "", ""
    lng_value, lat_value = [part.strip() for part in text_value.split(",", 1)]
    return lng_value, lat_value


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def haversine_m(lng1: float, lat1: float, lng2: float, lat2: float) -> float:
    radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)
    a_value = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * radius * math.atan2(math.sqrt(a_value), math.sqrt(1.0 - a_value))


def fetch_json(url: str, timeout: int = 30) -> tuple[dict[str, object], bool]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
    )
    default_context = ssl.create_default_context()
    insecure_context = ssl.create_default_context()
    insecure_context.check_hostname = False
    insecure_context.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(request, timeout=timeout, context=default_context) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}, True
    except urllib.error.URLError:
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=insecure_context) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else {}, True
        except Exception as exc:
            return {
                "status": "0",
                "infocode": "NETWORK_ERROR",
                "info": str(exc),
                "results": [],
            }, False
    except Exception as exc:
        return {
            "status": "0",
            "infocode": "NETWORK_ERROR",
            "info": str(exc),
            "results": [],
        }, False


def build_supply_tables() -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    nursery_rows = read_csv_rows(CLEAN_DIR / "nursery_master.csv")
    geo_rows = read_csv_rows(CLEAN_DIR / "geo_result.csv")
    nursery_by_id = {row["nursery_id"]: row for row in nursery_rows}
    geo_by_ref = {row["ref_id"]: row for row in geo_rows}

    usable_supply: list[dict[str, str]] = []
    excluded_supply: list[dict[str, str]] = []

    for nursery in nursery_rows:
        nursery_id = nursery.get("nursery_id", "")
        geo = geo_by_ref.get(nursery_id)
        if geo:
            lng_value = geo.get("lng_gcj02", "")
            lat_value = geo.get("lat_gcj02", "")
            geocode_level = geo.get("geocode_level", "")
            if lng_value and lat_value and geocode_level != "NO_RESULT":
                usable_supply.append(
                    {
                        "city": nursery.get("city", ""),
                        "nursery_id": nursery_id,
                        "nursery_name": nursery.get("institution_name_std", ""),
                        "nursery_district": nursery.get("district", "") or geo.get("district_name", ""),
                        "dest_lng": lng_value,
                        "dest_lat": lat_value,
                    }
                )
            else:
                excluded_supply.append(
                    {
                        "record_id": make_hash_id("exs", nursery_id, "no_result"),
                        "city": nursery.get("city", ""),
                        "nursery_id": nursery_id,
                        "institution_name_std": nursery.get("institution_name_std", ""),
                        "address_std": nursery.get("address_std", ""),
                        "exclude_reason": "GEOCODE_NO_RESULT",
                        "geo_status": nursery.get("geo_status", ""),
                        "review_status": nursery.get("review_status", ""),
                        "geocode_level": geocode_level,
                        "lng_gcj02": lng_value,
                        "lat_gcj02": lat_value,
                    }
                )
        else:
            excluded_supply.append(
                {
                    "record_id": make_hash_id("exs", nursery_id, "missing_geo"),
                    "city": nursery.get("city", ""),
                    "nursery_id": nursery_id,
                    "institution_name_std": nursery.get("institution_name_std", ""),
                    "address_std": nursery.get("address_std", ""),
                    "exclude_reason": "MISSING_GEO_RECORD",
                    "geo_status": nursery.get("geo_status", ""),
                    "review_status": nursery.get("review_status", ""),
                    "geocode_level": "",
                    "lng_gcj02": "",
                    "lat_gcj02": "",
                }
            )

    usable_supply.sort(key=lambda row: (row["city"], row["nursery_id"]))
    excluded_supply.sort(key=lambda row: (row["city"], row["nursery_id"]))
    for row in usable_supply:
        nursery_by_id[row["nursery_id"]] = nursery_by_id.get(row["nursery_id"], {})
    return usable_supply, excluded_supply


def load_demand_points() -> list[dict[str, str]]:
    rows = read_csv_rows(CLEAN_DIR / "poi_residential.csv")
    demand_points: list[dict[str, str]] = []
    for row in rows:
        lng_value, lat_value = parse_point(row.get("location_gcj02", ""))
        if not lng_value or not lat_value:
            continue
        demand_points.append(
            {
                "city": row.get("city", ""),
                "district": row.get("district", ""),
                "demand_poi_row_id": row.get("poi_row_id", ""),
                "demand_poi_id": row.get("poi_id", ""),
                "demand_name": row.get("poi_name", ""),
                "origin_lng": lng_value,
                "origin_lat": lat_value,
            }
        )
    return demand_points


def group_by_city(rows: list[dict[str, str]], city_key: str) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row.get(city_key, ""), []).append(row)
    return grouped


def select_top_k(
    demand_row: dict[str, str],
    supply_rows: list[dict[str, str]],
    top_k: int,
) -> list[dict[str, str]]:
    origin_lng = to_float(demand_row["origin_lng"])
    origin_lat = to_float(demand_row["origin_lat"])
    ranked_rows: list[dict[str, str]] = []
    for supply in supply_rows:
        euclid_distance_m = haversine_m(
            origin_lng,
            origin_lat,
            to_float(supply["dest_lng"]),
            to_float(supply["dest_lat"]),
        )
        ranked_rows.append({**supply, "euclid_distance_m": f"{euclid_distance_m:.2f}"})
    ranked_rows.sort(key=lambda row: float(row["euclid_distance_m"]))
    selected = ranked_rows[:top_k]
    for index, row in enumerate(selected, start=1):
        row["euclid_rank"] = str(index)
    return selected


def build_distance_url(
    api_key: str,
    mode: str,
    origin_lng: str,
    origin_lat: str,
    candidates: list[dict[str, str]],
) -> str:
    params = {
        "key": api_key,
        "origins": "|".join(f"{row['dest_lng']},{row['dest_lat']}" for row in candidates),
        "destination": f"{origin_lng},{origin_lat}",
        "type": "0",
    }
    endpoint = "https://restapi.amap.com/v3/distance"
    if mode != "walking":
        raise ValueError(f"Unsupported mode: {mode}")
    return endpoint + "?" + urllib.parse.urlencode(params)


def write_raw_response(request_id: str, payload: dict[str, object]) -> None:
    RAW_OD_DIR.mkdir(parents=True, exist_ok=True)
    (RAW_OD_DIR / f"{request_id}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def fetch_od_with_retry(request_url: str, timeout: int = 30) -> tuple[dict[str, object], bool]:
    backoffs = [0.5, 1.0, 2.0]
    payload: dict[str, object] = {}
    http_ok = False
    for attempt_index in range(len(backoffs) + 1):
        payload, http_ok = fetch_json(request_url, timeout=timeout)
        if str(payload.get("infocode", "")) != "10021":
            return payload, http_ok
        if attempt_index < len(backoffs):
            time.sleep(backoffs[attempt_index])
    return payload, http_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch AMap OD matrix for accessibility MVP.")
    parser.add_argument("--api-key", default="", help="Override AMap API key")
    parser.add_argument("--mode", default="walking", help="Travel mode; MVP supports walking only")
    parser.add_argument("--threshold", type=float, default=15.0, help="Coverage threshold in minutes")
    parser.add_argument("--top-k", type=int, default=8, help="Number of nearest supply candidates by euclidean distance")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    usable_supply, excluded_supply = build_supply_tables()
    write_csv_rows(OUTPUT_DIR / "excluded_supply_records.csv", EXCLUDED_FIELDS, excluded_supply)

    api_key = get_api_key(args.api_key)
    demand_points = load_demand_points()
    supply_by_city = group_by_city(usable_supply, "city")
    od_path = OUTPUT_DIR / "od_matrix_walk_15m.csv"
    request_log_path = OUTPUT_DIR / "od_request_log.csv"

    if not api_key:
        write_csv_rows(od_path, OD_FIELDS, [])
        write_csv_rows(request_log_path, REQUEST_LOG_FIELDS, [])
        print(
            "od fetch blocked: missing_api_key "
            f"usable_supply={len(usable_supply)} excluded_supply={len(excluded_supply)} demand_points={len(demand_points)}"
        )
        return

    existing_logs = read_csv_rows(request_log_path)
    successful_demand_ids = {
        row.get("demand_poi_row_id", "")
        for row in existing_logs
        if row.get("demand_poi_row_id", "")
        and row.get("api_status", "") == "1"
        and row.get("success_count", "") not in {"", "0"}
    }
    if existing_logs:
        demand_points = [
            row for row in demand_points if row["demand_poi_row_id"] not in successful_demand_ids
        ]
    else:
        write_csv_rows(od_path, OD_FIELDS, [])
        write_csv_rows(request_log_path, REQUEST_LOG_FIELDS, [])

    od_row_count = 0
    request_count = len(existing_logs)

    for demand_row in demand_points:
        city_supply = supply_by_city.get(demand_row["city"], [])
        if not city_supply:
            continue

        candidates = select_top_k(demand_row, city_supply, args.top_k)
        request_id = make_hash_id(
            "odreq",
            demand_row["city"],
            demand_row["demand_poi_row_id"],
            args.mode,
            args.top_k,
        )
        request_url = build_distance_url(
            api_key,
            args.mode,
            demand_row["origin_lng"],
            demand_row["origin_lat"],
            candidates,
        )
        payload, http_ok = fetch_od_with_retry(request_url, timeout=30)
        write_raw_response(request_id, payload)

        results = payload.get("results", []) if isinstance(payload.get("results"), list) else []
        success_count = 0
        failure_count = 0
        od_rows_for_request: list[dict[str, str]] = []

        for index, candidate in enumerate(candidates):
            result_row = results[index] if index < len(results) and isinstance(results[index], dict) else {}
            duration_sec = result_row.get("duration", "") if isinstance(result_row, dict) else ""
            distance_m = result_row.get("distance", "") if isinstance(result_row, dict) else ""
            if duration_sec not in ("", None):
                walk_time_min = f"{float(duration_sec) / 60.0:.2f}"
                od_status = "OK"
                success_count += 1
            else:
                walk_time_min = ""
                od_status = "NO_ROUTE"
                failure_count += 1
            od_rows_for_request.append(
                {
                    "od_row_id": make_hash_id(
                        "od",
                        demand_row["demand_poi_row_id"],
                        candidate["nursery_id"],
                        args.mode,
                    ),
                    "city": demand_row["city"],
                    "demand_poi_row_id": demand_row["demand_poi_row_id"],
                    "demand_poi_id": demand_row["demand_poi_id"],
                    "demand_name": demand_row["demand_name"],
                    "demand_district": demand_row["district"],
                    "origin_lng": demand_row["origin_lng"],
                    "origin_lat": demand_row["origin_lat"],
                    "nursery_id": candidate["nursery_id"],
                    "nursery_name": candidate["nursery_name"],
                    "nursery_district": candidate["nursery_district"],
                    "dest_lng": candidate["dest_lng"],
                    "dest_lat": candidate["dest_lat"],
                    "euclid_distance_m": candidate["euclid_distance_m"],
                    "euclid_rank": candidate["euclid_rank"],
                    "travel_mode": args.mode,
                    "threshold_min": f"{args.threshold:.0f}",
                    "walk_time_min": walk_time_min,
                    "walk_distance_m": str(distance_m or ""),
                    "od_status": od_status,
                    "request_id": request_id,
                }
            )

        append_csv_rows(od_path, OD_FIELDS, od_rows_for_request)
        append_csv_rows(
            request_log_path,
            REQUEST_LOG_FIELDS,
            [
                {
                    "request_id": request_id,
                    "request_time": now_ts(),
                    "city": demand_row["city"],
                    "demand_poi_row_id": demand_row["demand_poi_row_id"],
                    "demand_poi_id": demand_row["demand_poi_id"],
                    "demand_name": demand_row["demand_name"],
                    "candidate_count": str(len(candidates)),
                    "travel_mode": args.mode,
                    "request_url": request_url,
                    "api_status": str(payload.get("status", "")),
                    "infocode": str(payload.get("infocode", "")),
                    "result_count": str(len(results)),
                    "success_count": str(success_count),
                    "failure_count": str(failure_count),
                    "http_ok": "1" if http_ok else "0",
                }
            ],
        )
        od_row_count += len(od_rows_for_request)
        request_count += 1

    print(
        "od fetch complete: "
        f"demand_points={len(demand_points)} usable_supply={len(usable_supply)} "
        f"excluded_supply={len(excluded_supply)} requests={request_count} od_rows={od_row_count}"
    )


if __name__ == "__main__":
    main()

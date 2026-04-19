from __future__ import annotations

import argparse
import html
import re
from collections import Counter, defaultdict
from pathlib import Path

from pipeline_common import PROJECT_ROOT, read_csv_rows, write_csv_rows


OUTPUT_DIR = PROJECT_ROOT / "output" / "accessibility_mvp"
DEFAULT_CITY = "苏州"
TIME_PROXY_NOTE = "walk_time_proxy=network_distance_div_80m_per_min"

AUDIT_FIELDS = [
    "city",
    "nursery_id",
    "institution_name_std",
    "source_latest_id",
    "review_status",
    "geo_status",
    "district_master",
    "district_geo",
    "lng_gcj02",
    "lat_gcj02",
    "geocode_level",
    "usable_flag",
    "exclude_reason",
]


def quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return float("nan")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    left = int(position)
    right = min(left + 1, len(sorted_values) - 1)
    fraction = position - left
    return sorted_values[left] + (sorted_values[right] - sorted_values[left]) * fraction


def format_number(value: float) -> str:
    if value != value:
        return ""
    return f"{value:.2f}"


def top_counter_ratio(counter: Counter[str], denominator: int) -> tuple[str, int, float]:
    if not counter or denominator <= 0:
        return "", 0, 0.0
    name, count = counter.most_common(1)[0]
    return name, count, count / denominator


def load_snapshot_stats(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "snapshot_exists": 0,
            "snapshot_row_count": 0,
            "snapshot_has_pagination_text": 0,
        }
    text = path.read_text(encoding="utf-8", errors="ignore")
    row_count = max(len(re.findall(r"(?is)<tr\b", text)) - 1, 0)
    lowered = text.lower()
    has_pagination = int(
        any(token in lowered for token in ("page=", "pageno", "pagesize", "total", "分页"))
    )
    return {
        "snapshot_exists": 1,
        "snapshot_row_count": row_count,
        "snapshot_has_pagination_text": has_pagination,
    }


def load_blocker_count(path: Path, marker: str) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8", errors="ignore")
    return text.count(marker)


def build_supply_rows(city: str) -> list[dict[str, str]]:
    nursery_rows = read_csv_rows(PROJECT_ROOT / "clean" / "nursery_master.csv")
    geo_rows = read_csv_rows(PROJECT_ROOT / "clean" / "geo_result.csv")
    geo_by_ref = {row["ref_id"]: row for row in geo_rows}

    audit_rows: list[dict[str, str]] = []
    for row in nursery_rows:
        if row.get("city", "") != city:
            continue
        geo = geo_by_ref.get(row.get("nursery_id", ""), {})
        lng_value = geo.get("lng_gcj02", "")
        lat_value = geo.get("lat_gcj02", "")
        geocode_level = geo.get("geocode_level", "")
        usable_flag = "1" if lng_value and lat_value and geocode_level != "NO_RESULT" else "0"
        if usable_flag == "1":
            exclude_reason = ""
        elif geo:
            exclude_reason = "GEOCODE_NO_RESULT"
        else:
            exclude_reason = "MISSING_GEO_RECORD"
        audit_rows.append(
            {
                "city": city,
                "nursery_id": row.get("nursery_id", ""),
                "institution_name_std": row.get("institution_name_std", ""),
                "source_latest_id": row.get("source_latest_id", ""),
                "review_status": row.get("review_status", ""),
                "geo_status": row.get("geo_status", ""),
                "district_master": row.get("district", ""),
                "district_geo": geo.get("district_name", ""),
                "lng_gcj02": lng_value,
                "lat_gcj02": lat_value,
                "geocode_level": geocode_level,
                "usable_flag": usable_flag,
                "exclude_reason": exclude_reason,
            }
        )
    audit_rows.sort(key=lambda row: (row["usable_flag"], row["source_latest_id"], row["nursery_id"]), reverse=True)
    return audit_rows


def build_demand_rows(city: str) -> list[dict[str, str]]:
    return [row for row in read_csv_rows(PROJECT_ROOT / "clean" / "poi_residential.csv") if row.get("city", "") == city]


def build_point_rows(city: str) -> list[dict[str, str]]:
    return [row for row in read_csv_rows(OUTPUT_DIR / "accessibility_point_mvp.csv") if row.get("city", "") == city]


def build_diagnosis(city: str) -> tuple[list[dict[str, str]], str]:
    supply_rows = build_supply_rows(city)
    demand_rows = build_demand_rows(city)
    point_rows = build_point_rows(city)

    usable_supply = [row for row in supply_rows if row["usable_flag"] == "1"]
    excluded_supply = [row for row in supply_rows if row["usable_flag"] == "0"]

    source_counter = Counter(row["source_latest_id"] or "(blank)" for row in supply_rows)
    district_counter = Counter(row["district_geo"] or "(blank)" for row in usable_supply)
    nearest_counter = Counter(row["nearest_nursery_name"] or "(blank)" for row in point_rows)

    source_name, source_count, source_ratio = top_counter_ratio(source_counter, len(supply_rows))
    district_name, district_count, district_ratio = top_counter_ratio(district_counter, len(usable_supply))
    nearest_name, nearest_count, nearest_ratio = top_counter_ratio(nearest_counter, len(point_rows))

    source_singleton_risk = 1 if source_ratio >= 0.8 else 0
    spatial_concentration_risk = 1 if district_ratio >= 0.8 else 0
    nearest_domination_risk = 1 if nearest_ratio >= 0.7 else 0
    valid_for_citywide_result = 0 if (source_singleton_risk + spatial_concentration_risk + nearest_domination_risk) >= 2 else 1

    snapshot_stats = load_snapshot_stats(PROJECT_ROOT / "raw_official" / "registry" / "SZ_REG_MAP_2024_JSON_9F1DD629.html")
    blocker_count = load_blocker_count(PROJECT_ROOT / "logs" / "blockers.md", "SZ_REG_MAP_2024")

    demand_unique_ids = {row["poi_row_id"] for row in demand_rows}
    demand_districts = sorted({row.get("district", "") for row in demand_rows})
    covered_count = sum(1 for row in point_rows if row.get("covered_15m", "") == "1")

    district_times: dict[str, list[float]] = defaultdict(list)
    district_covered: Counter[str] = Counter()
    district_nearest: dict[str, Counter[str]] = defaultdict(Counter)
    for row in point_rows:
        district_name_key = row.get("district", "") or "(blank)"
        if row.get("nearest_walk_time_min", ""):
            district_times[district_name_key].append(float(row["nearest_walk_time_min"]))
        if row.get("covered_15m", "") == "1":
            district_covered[district_name_key] += 1
        district_nearest[district_name_key][row.get("nearest_nursery_name", "") or "(blank)"] += 1

    lines = [
        "Suzhou supply completeness audit",
        f"city={city}",
        TIME_PROXY_NOTE,
        f"supply_total={len(supply_rows)}",
        f"supply_usable={len(usable_supply)}",
        f"supply_excluded={len(excluded_supply)}",
        f"demand_raw_rows={len(demand_rows)}",
        f"demand_unique_points={len(demand_unique_ids)}",
        f"demand_district_count={len(demand_districts)}",
        f"point_rows={len(point_rows)}",
        f"covered_15m={covered_count}",
        f"snapshot_exists={snapshot_stats['snapshot_exists']}",
        f"snapshot_row_count={snapshot_stats['snapshot_row_count']}",
        f"snapshot_has_pagination_text={snapshot_stats['snapshot_has_pagination_text']}",
        f"blocker_occurrences_SZ_REG_MAP_2024={blocker_count}",
        "",
        "source_distribution:",
    ]
    for name, count in source_counter.most_common():
        lines.append(f"- {name}: {count}")

    lines.append("")
    lines.append("usable_supply_district_distribution:")
    for name, count in district_counter.most_common():
        lines.append(f"- {name}: {count}")

    lines.append("")
    lines.append("top_nearest_nursery_distribution:")
    for name, count in nearest_counter.most_common(10):
        lines.append(f"- {name}: {count}")

    lines.append("")
    lines.append("district_level_summary:")
    for district_name_key in sorted(district_times):
        values = sorted(district_times[district_name_key])
        top_nearest_name, top_nearest_count, _ = top_counter_ratio(district_nearest[district_name_key], len(values))
        coverage = district_covered[district_name_key] / len(values) * 100.0 if values else float("nan")
        lines.append(
            "- "
            + f"{district_name_key}: n={len(values)} "
            + f"median={format_number(quantile(values, 0.5))} "
            + f"p25={format_number(quantile(values, 0.25))} "
            + f"p75={format_number(quantile(values, 0.75))} "
            + f"coverage_15m={format_number(coverage)} "
            + f"top_nearest={top_nearest_name} ({top_nearest_count})"
        )

    lines.extend(
        [
            "",
            "risk_flags:",
            f"source_singleton_risk={source_singleton_risk} ratio={format_number(source_ratio * 100.0)}",
            f"spatial_concentration_risk={spatial_concentration_risk} ratio={format_number(district_ratio * 100.0)}",
            f"nearest_domination_risk={nearest_domination_risk} ratio={format_number(nearest_ratio * 100.0)}",
            f"valid_for_citywide_result={valid_for_citywide_result}",
            (
                "judgement=source_incomplete_and_spatially_concentrated"
                if valid_for_citywide_result == 0
                else "judgement=no_citywide_invalidity_detected"
            ),
        ]
    )

    return supply_rows, "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Suzhou supply completeness and anomaly credibility.")
    parser.add_argument("--city", default=DEFAULT_CITY, help="Target city name")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    supply_rows, diagnosis_text = build_diagnosis(args.city)
    write_csv_rows(OUTPUT_DIR / "suzhou_supply_audit.csv", AUDIT_FIELDS, supply_rows)
    (OUTPUT_DIR / "suzhou_anomaly_diagnosis.txt").write_text(diagnosis_text, encoding="utf-8")
    print(diagnosis_text, end="")


if __name__ == "__main__":
    main()

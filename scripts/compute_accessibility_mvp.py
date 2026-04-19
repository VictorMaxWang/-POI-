from __future__ import annotations

import argparse
from statistics import mean

from pipeline_common import PROJECT_ROOT, write_csv_rows

from fetch_od_matrix import OD_FIELDS


OUTPUT_DIR = PROJECT_ROOT / "output" / "accessibility_mvp"
WALK_METERS_PER_MIN = 80.0

POINT_FIELDS = [
    "city",
    "district",
    "demand_poi_row_id",
    "demand_poi_id",
    "demand_name",
    "nearest_nursery_id",
    "nearest_nursery_name",
    "nearest_walk_time_min",
    "covered_15m",
]

CITY_FIELDS = [
    "city",
    "demand_points",
    "matched_points",
    "median_nearest_time",
    "p25_nearest_time",
    "p75_nearest_time",
    "mean_nearest_time",
    "coverage_15m",
]


def read_od_rows(path: str) -> list[dict[str, str]]:
    import csv

    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


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


def effective_walk_time_min(row: dict[str, str]) -> float | None:
    raw_time = row.get("walk_time_min", "")
    if raw_time not in {"", None}:
        time_value = float(raw_time)
        if time_value > 0:
            return time_value
    raw_distance = row.get("walk_distance_m", "")
    if raw_distance not in {"", None}:
        distance_value = float(raw_distance)
        if distance_value > 0:
            return distance_value / WALK_METERS_PER_MIN
    return None


def compute_2sfca(point_rows: list[dict[str, str]], threshold: float) -> dict[str, float]:
    covered_by_supply: dict[str, list[str]] = {}
    for row in point_rows:
        if row["nearest_walk_time_min"] == "":
            continue
        if float(row["nearest_walk_time_min"]) <= threshold:
            covered_by_supply.setdefault(row["nearest_nursery_id"], []).append(row["demand_poi_row_id"])
    supply_ratio = {
        nursery_id: (1.0 / len(demand_ids))
        for nursery_id, demand_ids in covered_by_supply.items()
        if demand_ids
    }
    point_score: dict[str, float] = {}
    for row in point_rows:
        if row["covered_15m"] == "1" and row["nearest_nursery_id"] in supply_ratio:
            point_score[row["demand_poi_row_id"]] = supply_ratio[row["nearest_nursery_id"]]
        else:
            point_score[row["demand_poi_row_id"]] = 0.0
    return point_score


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute accessibility MVP summaries from OD matrix.")
    parser.add_argument(
        "--input",
        default=str(OUTPUT_DIR / "od_matrix_walk_15m.csv"),
        help="OD matrix CSV path",
    )
    parser.add_argument("--threshold", type=float, default=15.0, help="Coverage threshold in minutes")
    parser.add_argument("--with-2sfca", action="store_true", help="Append lightweight 2SFCA proxy score")
    args = parser.parse_args()

    od_rows = read_od_rows(args.input)
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in od_rows:
        grouped.setdefault(row["demand_poi_row_id"], []).append(row)

    point_rows: list[dict[str, str]] = []
    for demand_id, rows in grouped.items():
        ok_rows = []
        for row in rows:
            effective_time = effective_walk_time_min(row)
            if row.get("od_status") == "OK" and effective_time is not None:
                ok_rows.append((row, effective_time))
        best_pair = min(ok_rows, key=lambda item: item[1]) if ok_rows else None
        best_row = best_pair[0] if best_pair else None
        best_time = best_pair[1] if best_pair else None
        point_row = {
            "city": rows[0].get("city", ""),
            "district": rows[0].get("demand_district", ""),
            "demand_poi_row_id": demand_id,
            "demand_poi_id": rows[0].get("demand_poi_id", ""),
            "demand_name": rows[0].get("demand_name", ""),
            "nearest_nursery_id": best_row.get("nursery_id", "") if best_row else "",
            "nearest_nursery_name": best_row.get("nursery_name", "") if best_row else "",
            "nearest_walk_time_min": format_number(best_time) if best_time is not None else "",
            "covered_15m": (
                "1"
                if best_time is not None and best_time <= args.threshold
                else "0"
            ),
        }
        point_rows.append(point_row)

    if args.with_2sfca:
        point_score = compute_2sfca(point_rows, args.threshold)
        for row in point_rows:
            row["accessibility_2sfca_proxy"] = format_number(point_score[row["demand_poi_row_id"]])
        point_fields = POINT_FIELDS + ["accessibility_2sfca_proxy"]
    else:
        point_fields = POINT_FIELDS

    point_rows.sort(key=lambda row: (row["city"], row["district"], row["demand_poi_row_id"]))
    write_csv_rows(OUTPUT_DIR / "accessibility_point_mvp.csv", point_fields, point_rows)

    city_rows: list[dict[str, str]] = []
    for city in sorted({row["city"] for row in point_rows}):
        city_points = [row for row in point_rows if row["city"] == city]
        matched = [row for row in city_points if row["nearest_walk_time_min"]]
        times = sorted(float(row["nearest_walk_time_min"]) for row in matched)
        covered = [row for row in city_points if row["covered_15m"] == "1"]
        city_rows.append(
            {
                "city": city,
                "demand_points": str(len(city_points)),
                "matched_points": str(len(matched)),
                "median_nearest_time": format_number(quantile(times, 0.5)),
                "p25_nearest_time": format_number(quantile(times, 0.25)),
                "p75_nearest_time": format_number(quantile(times, 0.75)),
                "mean_nearest_time": format_number(mean(times) if times else float("nan")),
                "coverage_15m": format_number((len(covered) / len(city_points) * 100.0) if city_points else float("nan")),
            }
        )

    write_csv_rows(OUTPUT_DIR / "accessibility_city_summary.csv", CITY_FIELDS, city_rows)

    lines = [
        "Accessibility MVP run summary",
        f"input={args.input}",
        f"threshold_min={args.threshold:.0f}",
        f"walk_time_proxy=network_distance_div_{int(WALK_METERS_PER_MIN)}m_per_min_when_duration_is_zero",
        f"point_rows={len(point_rows)}",
        f"city_rows={len(city_rows)}",
        "",
    ]
    for row in city_rows:
        lines.append(
            f"{row['city']}: demand={row['demand_points']} matched={row['matched_points']} "
            f"median={row['median_nearest_time']} coverage_15m={row['coverage_15m']}%"
        )
    (OUTPUT_DIR / "accessibility_run_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"accessibility compute complete: point_rows={len(point_rows)} city_rows={len(city_rows)}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import sys

from city_registry_config import registry_city_names
from pipeline_common import CLEAN_DIR, LOGS_DIR, make_hash_id, read_csv_rows, schema_fieldnames, write_csv_rows


OUTPUT_FIELDS = schema_fieldnames("registry_minimums_report.csv")
TARGET_CITIES = registry_city_names()


def required_raw_fields_present(row: dict[str, str]) -> bool:
    return all(
        [
            row.get("institution_name_raw", "").strip(),
            row.get("address_raw", "").strip(),
            row.get("source_id", "").strip(),
            row.get("source_url", "").strip(),
        ]
    )


def add_check(
    rows: list[dict[str, str]],
    scope_type: str,
    scope_value: str,
    metric_name: str,
    metric_value: str,
    threshold: str,
    passed: bool,
    note: str = "",
) -> None:
    rows.append(
        {
            "check_id": make_hash_id("check", scope_type, scope_value, metric_name),
            "scope_type": scope_type,
            "scope_value": scope_value,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "threshold": threshold,
            "pass_flag": "1" if passed else "0",
            "note": note,
        }
    )


def main() -> None:
    raw_rows = read_csv_rows(CLEAN_DIR / "nursery_registry_raw.csv")
    master_rows = read_csv_rows(CLEAN_DIR / "nursery_master.csv")
    report_rows: list[dict[str, str]] = []

    raw_complete = [row for row in raw_rows if required_raw_fields_present(row)]
    raw_ratio = (len(raw_complete) / len(raw_rows)) if raw_rows else 0.0
    ready_master = [row for row in master_rows if row.get("review_status") == "READY_FOR_GEOCODE"]
    ready_ratio = (len(ready_master) / len(master_rows)) if master_rows else 0.0

    add_check(report_rows, "pipeline", "ALL", "raw_total_rows", str(len(raw_rows)), ">=20", len(raw_rows) >= 20)
    add_check(report_rows, "pipeline", "ALL", "raw_required_field_ratio", f"{raw_ratio:.4f}", ">=0.80", raw_ratio >= 0.80)
    add_check(report_rows, "pipeline", "ALL", "master_total_rows", str(len(master_rows)), ">=4", len(master_rows) >= 4)
    add_check(report_rows, "pipeline", "ALL", "master_ready_ratio", f"{ready_ratio:.4f}", ">=0.70", ready_ratio >= 0.70)

    for city in TARGET_CITIES:
        city_raw = [row for row in raw_rows if row.get("city") == city]
        city_master = [row for row in master_rows if row.get("city") == city]
        add_check(report_rows, "city", city, "raw_city_rows", str(len(city_raw)), ">=1", len(city_raw) >= 1)
        add_check(report_rows, "city", city, "master_city_rows", str(len(city_master)), ">=1", len(city_master) >= 1)

    write_csv_rows(LOGS_DIR / "registry_minimums_report.csv", OUTPUT_FIELDS, report_rows)
    failed = [row for row in report_rows if row.get("pass_flag") != "1"]
    print(
        "registry_minimums "
        f"raw_rows={len(raw_rows)} master_rows={len(master_rows)} "
        f"checks={len(report_rows)} failed={len(failed)}"
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

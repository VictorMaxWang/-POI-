from __future__ import annotations

import argparse
import re

from pipeline_common import (
    CLEAN_DIR,
    RAW_OFFICIAL_DIR,
    decode_bytes,
    ensure_standard_files,
    extract_tables_from_html,
    extract_visible_text,
    load_html_for_source,
    load_manifest,
    make_hash_id,
    maybe_number,
    normalize_whitespace,
    schema_fieldnames,
    select_source_url,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("population_city_district.csv")


def add_row(
    rows: list[dict[str, str]],
    *,
    city: str,
    district: str,
    stat_year: str,
    source_id: str,
    source_url: str,
    indicator_name: str,
    indicator_value: str,
    indicator_unit: str,
    population_scope: str = "",
    age_group: str = "",
    sex_group: str = "",
    urban_rural: str = "",
    note_raw: str = "",
    extract_method: str = "",
    manual_check_flag: str = "0",
) -> None:
    indicator_value = maybe_number(indicator_value)
    if not indicator_value:
        return
    rows.append(
        {
            "pop_record_id": make_hash_id(
                "pop",
                city,
                district,
                stat_year,
                source_id,
                indicator_name,
                age_group,
                indicator_value,
            ),
            "city": city,
            "district": district,
            "stat_year": stat_year,
            "source_id": source_id,
            "source_url": source_url,
            "indicator_name": indicator_name,
            "indicator_value": indicator_value,
            "indicator_unit": indicator_unit,
            "population_scope": population_scope,
            "age_group": age_group,
            "sex_group": sex_group,
            "urban_rural": urban_rural,
            "note_raw": note_raw,
            "extract_method": extract_method,
            "manual_check_flag": manual_check_flag,
        }
    )


def normalize_district(city: str, label: str) -> str:
    label = normalize_whitespace(label)
    if label in {"", "全市", city, f"{city}市"}:
        return ""
    return label


def is_numericish(value: str) -> bool:
    return bool(re.search(r"\d", value))


def parse_region_table(city: str, source_id: str, source_url: str, html_text: str) -> list[dict[str, str]]:
    rows = []
    tables = extract_tables_from_html(html_text)
    for table in tables:
        if len(table) < 3:
            continue
        header_text = " ".join(" ".join(row) for row in table[:2])
        if "地区" not in header_text and "人口" not in header_text and "常住人口" not in header_text:
            continue
        for table_row in table[2:]:
            if len(table_row) < 2:
                continue
            district_raw = table_row[0]
            population_value = table_row[1]
            if not is_numericish(population_value):
                continue
            district = normalize_district(city, district_raw)
            add_row(
                rows,
                city=city,
                district=district,
                stat_year="2020",
                source_id=source_id,
                source_url=source_url,
                indicator_name="resident_population",
                indicator_value=population_value,
                indicator_unit="person",
                population_scope="resident",
                note_raw=district_raw,
                extract_method="table_regex",
            )
            if len(table_row) >= 3 and is_numericish(table_row[2]):
                add_row(
                    rows,
                    city=city,
                    district=district,
                    stat_year="2020",
                    source_id=source_id,
                    source_url=source_url,
                    indicator_name="resident_population_share",
                    indicator_value=table_row[2],
                    indicator_unit="percent",
                    population_scope="resident",
                    note_raw="2020 share",
                    extract_method="table_regex",
                )
            if len(table_row) >= 4 and is_numericish(table_row[3]):
                add_row(
                    rows,
                    city=city,
                    district=district,
                    stat_year="2010",
                    source_id=source_id,
                    source_url=source_url,
                    indicator_name="resident_population_share",
                    indicator_value=table_row[3],
                    indicator_unit="percent",
                    population_scope="resident",
                    note_raw="2010 share",
                    extract_method="table_regex",
                    manual_check_flag="1",
                )
        if rows:
            break
    return rows


def normalize_age_group(label: str) -> str:
    label = normalize_whitespace(label)
    if "0-14" in label:
        return "0-14"
    if "15-59" in label:
        return "15-59"
    if "65" in label:
        return "65+"
    if "60" in label:
        return "60+"
    if "总" in label:
        return "all"
    return label


def parse_age_totals(city: str, source_id: str, source_url: str, html_text: str) -> list[dict[str, str]]:
    rows = []
    tables = extract_tables_from_html(html_text)
    for table in tables:
        if len(table) < 4:
            continue
        header_text = " ".join(" ".join(row) for row in table[:2])
        if "年龄" not in header_text and "年 龄" not in header_text:
            continue
        for table_row in table[1:]:
            if len(table_row) < 2:
                continue
            if not is_numericish(table_row[1]):
                continue
            age_group = normalize_age_group(table_row[0])
            add_row(
                rows,
                city=city,
                district="",
                stat_year="2020",
                source_id=source_id,
                source_url=source_url,
                indicator_name="resident_population",
                indicator_value=table_row[1],
                indicator_unit="person",
                population_scope="resident",
                age_group=age_group,
                note_raw=table_row[0],
                extract_method="table_regex",
            )
            if len(table_row) >= 3 and is_numericish(table_row[2]):
                add_row(
                    rows,
                    city=city,
                    district="",
                    stat_year="2020",
                    source_id=source_id,
                    source_url=source_url,
                    indicator_name="age_share_of_population",
                    indicator_value=table_row[2],
                    indicator_unit="percent",
                    population_scope="resident",
                    age_group=age_group,
                    note_raw=table_row[0],
                    extract_method="table_regex",
                )
        if rows:
            break
    return rows


def parse_nantong_age_distribution(source_url: str, html_text: str) -> list[dict[str, str]]:
    rows = []
    tables = extract_tables_from_html(html_text)
    for table in tables:
        if len(table) < 8:
            continue
        header_text = " ".join(" ".join(row) for row in table[:4])
        if "0-14岁" not in header_text or "60岁及以上" not in header_text:
            continue
        for table_row in table:
            if len(table_row) < 5:
                continue
            if not is_numericish(table_row[1]) or not is_numericish(table_row[2]):
                continue
            district = normalize_district("南通", table_row[0])
            for age_group, value in zip(
                ["0-14", "15-59", "60+", "65+"],
                table_row[1:5],
            ):
                add_row(
                    rows,
                    city="南通",
                    district=district,
                    stat_year="2020",
                    source_id="NT_POP_7C_AGE_2021",
                    source_url=source_url,
                    indicator_name="age_share_of_population",
                    indicator_value=value,
                    indicator_unit="percent",
                    population_scope="resident",
                    age_group=age_group,
                    note_raw="地区年龄结构占比",
                    extract_method="table_regex",
                )
        if rows:
            break
    return rows


def parse_bulletin_metrics(city: str, stat_year: str, source_id: str, source_url: str, html_text: str) -> list[dict[str, str]]:
    text_value = normalize_whitespace(extract_visible_text(html_text))
    rows = []
    patterns = [
        ("resident_population", "resident", "all", [r"(?:年末)?常住人口(?:为|达|有)?([0-9.]+)(万人|人)"]),
        ("urbanization_rate", "", "all", [r"城镇化率(?:为|达)?([0-9.]+)(%|％)"]),
        ("birth_rate", "resident", "all", [r"(?:人口)?出生率(?:为|达)?([0-9.]+)(‰|%|％)"]),
        ("death_rate", "resident", "all", [r"(?:人口)?死亡率(?:为|达)?([0-9.]+)(‰|%|％)"]),
    ]
    for indicator_name, population_scope, age_group, pattern_list in patterns:
        for pattern in pattern_list:
            match = re.search(pattern, text_value)
            if match:
                add_row(
                    rows,
                    city=city,
                    district="",
                    stat_year=stat_year,
                    source_id=source_id,
                    source_url=source_url,
                    indicator_name=indicator_name,
                    indicator_value=match.group(1),
                    indicator_unit=match.group(2),
                    population_scope=population_scope,
                    age_group=age_group,
                    note_raw="bulletin_regex",
                    extract_method="regex_text",
                    manual_check_flag="1",
                )
                break
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse fetched population pages into clean CSV.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    args = parser.parse_args()

    ensure_standard_files()
    manifest = [row for row in load_manifest() if row.get("target_table") == "population_city_district"]
    if args.city:
        manifest = [row for row in manifest if row.get("city") in set(args.city)]

    output_rows: list[dict[str, str]] = []
    for row in manifest:
        html_text = load_html_for_source(row["source_id"], RAW_OFFICIAL_DIR / "population")
        if not html_text:
            continue
        source_id = row["source_id"]
        source_url = select_source_url(row)
        city = row["city"]
        if source_id in {"SZ_POP_7C_REGION_2021", "NT_POP_7C_REGION_2021", "NJ_POP_7C_2021"}:
            output_rows.extend(parse_region_table(city, source_id, source_url, html_text))
            output_rows.extend(parse_age_totals(city, source_id, source_url, html_text))
        elif source_id in {"SZ_POP_7C_AGE_2021", "NT_POP_7C_AGE_2021"}:
            output_rows.extend(parse_age_totals(city, source_id, source_url, html_text))
            if source_id == "NT_POP_7C_AGE_2021":
                output_rows.extend(parse_nantong_age_distribution(source_url, html_text))
        elif source_id.endswith("_GB_2024"):
            output_rows.extend(parse_bulletin_metrics(city, "2024", source_id, source_url, html_text))

    seen = set()
    deduped_rows = []
    for row in output_rows:
        key = tuple(row[field] for field in OUTPUT_FIELDS)
        if key in seen:
            continue
        seen.add(key)
        deduped_rows.append(row)

    write_csv_rows(CLEAN_DIR / "population_city_district.csv", OUTPUT_FIELDS, deduped_rows)
    print(f"population_parse complete: rows={len(deduped_rows)}")


if __name__ == "__main__":
    main()

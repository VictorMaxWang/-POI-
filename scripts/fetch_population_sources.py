from __future__ import annotations

import argparse
import urllib.parse

from pipeline_common import (
    RAW_OFFICIAL_DIR,
    append_blocker,
    ensure_standard_files,
    fetch_url,
    guess_extension,
    load_manifest,
    log_fetch,
    save_response,
    select_source_url,
)


def iter_population_sources(cities: set[str], source_ids: set[str]) -> list[dict[str, str]]:
    rows = []
    for row in load_manifest():
        if row.get("target_table") != "population_city_district":
            continue
        if cities and row.get("city") not in cities:
            continue
        if source_ids and row.get("source_id") not in source_ids:
            continue
        rows.append(row)
    return rows


def build_referer(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/" if parsed.scheme and parsed.netloc else ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch population-related official sources.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    parser.add_argument("--source-id", nargs="*", default=[], help="Limit to source IDs")
    args = parser.parse_args()

    ensure_standard_files()
    sources = iter_population_sources(set(args.city), set(args.source_id))
    fetched = 0
    blocked = 0

    for row in sources:
        url = select_source_url(row)
        if not url.startswith("http"):
            append_blocker(
                "population_fetch",
                row,
                "url_missing_or_non_http",
                "核对 source_manifest.csv 中的来源地址；如为纯页面名，请人工补充真实 URL。",
            )
            blocked += 1
            continue

        result = fetch_url(url, referer=build_referer(url))
        local_path = None
        note = ""
        if result.get("content"):
            extension = guess_extension(str(result.get("content_type", "")), url)
            local_path = save_response(result["content"], RAW_OFFICIAL_DIR / "population", row["source_id"], extension)
            fetched += 1

        blocker_reason = str(result.get("blocker_reason", ""))
        if blocker_reason:
            blocked += 1
            manual_action = (
                "优先人工下载官方页面/PDF并放入 raw_official/population/，"
                "保留截图；若为 WAF，请更换网络环境后补抓。"
            )
            append_blocker("population_fetch", row, blocker_reason, manual_action)
            note = f"blocker:{blocker_reason}"
        elif row.get("access_method", "").startswith("manual_"):
            append_blocker(
                "population_fetch",
                row,
                f"access_method={row.get('access_method')}",
                "该来源以人工下载为主；如自动抓取结果不完整，请人工补落官方附件。",
            )
            note = f"manual_hint:{row.get('access_method')}"

        log_fetch("population_fetch", row, result, local_path, note=note)

    print(f"population_fetch complete: sources={len(sources)} fetched_files={fetched} blocked={blocked}")


if __name__ == "__main__":
    main()

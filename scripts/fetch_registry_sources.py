from __future__ import annotations

import argparse
import urllib.parse

from pipeline_common import (
    RAW_OFFICIAL_DIR,
    append_blocker,
    ensure_standard_files,
    extract_links,
    fetch_url,
    guess_extension,
    load_manifest,
    log_fetch,
    save_response,
    seed_manual_capture_row,
    select_source_url,
)


def iter_registry_sources(cities: set[str], source_ids: set[str]) -> list[dict[str, str]]:
    rows = []
    for row in load_manifest():
        if row.get("target_table") != "nursery_registry_raw":
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
    parser = argparse.ArgumentParser(description="Fetch registry entry pages and official list pages.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    parser.add_argument("--source-id", nargs="*", default=[], help="Limit to source IDs")
    args = parser.parse_args()

    ensure_standard_files()
    sources = iter_registry_sources(set(args.city), set(args.source_id))
    fetched = 0
    blocked = 0

    for row in sources:
        url = select_source_url(row)
        if not url.startswith("http"):
            append_blocker(
                "registry_fetch",
                row,
                "url_missing_or_non_http",
                "核对 source_manifest.csv 中的来源地址；如仅有平台名，请人工记录入口路径和截图。",
            )
            blocked += 1
            continue

        result = fetch_url(url, referer=build_referer(url))
        local_path = None
        note = ""
        html_text = ""
        if result.get("content"):
            extension = guess_extension(str(result.get("content_type", "")), url)
            local_path = save_response(result["content"], RAW_OFFICIAL_DIR / "registry", row["source_id"], extension)
            fetched += 1
            if extension in {".html", ".htm", ".txt"}:
                html_text = result["content"].decode("utf-8", errors="ignore")

        blocker_reason = str(result.get("blocker_reason", ""))
        if blocker_reason:
            blocked += 1
            if blocker_reason == "http_404":
                manual_action = "核对页面是否失效或迁移；保留当前入口页截图，并在同站点人工搜索新链接后补录。"
            elif blocker_reason.startswith("http_403") or "WAF" in blocker_reason:
                manual_action = "保留阻塞截图；如为 WAF，请更换网络环境后重试，仍失败则人工复制官方名单。"
            else:
                manual_action = "保留阻塞截图并人工补录；如页面结构不稳定，请直接转人工流程。"
            append_blocker(
                "registry_fetch",
                row,
                blocker_reason,
                manual_action,
            )
            seed_manual_capture_row(row, f"来源阻塞：{blocker_reason}")
            note = f"blocker:{blocker_reason}"
        elif row.get("access_method") in {"manual_app_capture", "manual_browser_capture", "whitelist_crawl_or_manual"}:
            blocked += 1
            append_blocker(
                "registry_fetch",
                row,
                f"access_method={row.get('access_method')}",
                "该来源是 App/H5/白名单人工流程；先抓官方入口页，再人工补录机构明细。",
            )
            seed_manual_capture_row(row, f"需人工补录：{row.get('access_method')}")
            note = f"manual_hint:{row.get('access_method')}"
        elif row.get("source_type") == "registry_entry" and html_text:
            links = extract_links(html_text, url)
            if not links:
                append_blocker(
                    "registry_fetch",
                    row,
                    "entry_page_without_links",
                    "入口页未识别到稳定子链接；请人工打开页面并补录区级入口。",
                )
                blocked += 1
                seed_manual_capture_row(row, "入口页无稳定二级链接，请人工补录区级页面。")
            else:
                note = f"entry_links_detected:{len(links)}"

        log_fetch("registry_fetch", row, result, local_path, note=note)

    print(f"registry_fetch complete: sources={len(sources)} fetched_files={fetched} blocked={blocked}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import html
import re
import urllib.parse

from city_registry_config import iter_registry_source_slots
from pipeline_common import (
    RAW_OFFICIAL_DIR,
    decode_bytes,
    extract_links,
    extract_tables_from_html,
    extract_title,
    extract_visible_text,
    fetch_url,
    get_manifest_row,
    load_html_for_source,
    log_registry_probe,
    normalize_whitespace,
    seed_manual_capture_row,
    select_source_url,
    upsert_manifest_rows,
)


NANJING_PROBE_SEEDS = {
    ("秦淮区", "秦淮区0-3岁备案托育机构公示"): "https://www.njqh.gov.cn/qhqrmzf/202507/t20250711_5605267.html",
    ("江宁区", "江宁区已审批备案托育机构公示"): "https://www.jiangning.gov.cn/jnqrmzf/202405/t20240515_4666588.html",
    ("浦口区", "浦口区备案托育机构公示"): "https://www.pukou.gov.cn/pkqrmzf/202309/t20230915_4012368.html",
    ("浦口区", "南京市浦口区备案托育机构公示"): "https://www.pukou.gov.cn/pkqrmzf/202309/t20230915_4012368.html",
}

SUZHOU_H5_SEEDS = [
    {
        "source_id": "SZ_REG_H5_HEALTH_SUZHOU",
        "source_name": "健康苏州掌上行 > 苏州托育地图",
        "url_or_page_name": "健康苏州掌上行 App > 苏州托育地图",
        "access_method": "manual_app_capture",
        "page_role": "platform_h5_probe",
        "source_status": "manual_required",
        "official_or_platform": "official-linked platform",
        "notes": "优先导出公开 H5 HAR 或 HTML 快照，不直接补机构字段。",
    },
    {
        "source_id": "SZ_REG_H5_SUZHOUDAO",
        "source_name": "苏周到 > 苏州托育地图",
        "url_or_page_name": "苏周到 App > 苏州托育地图",
        "access_method": "manual_app_capture",
        "page_role": "platform_h5_probe",
        "source_status": "manual_required",
        "official_or_platform": "official-linked platform",
        "notes": "优先记录最终公开 H5 URL 或导出 HAR。",
    },
    {
        "source_id": "SZ_REG_WECHAT_SUZHOU_HEALTH",
        "source_name": "苏州健康公众号 > 苏州托育地图",
        "url_or_page_name": "苏州健康公众号 > 苏州托育地图",
        "access_method": "manual_wechat_capture",
        "page_role": "wechat_h5_probe",
        "source_status": "manual_required",
        "official_or_platform": "official-linked platform",
        "notes": "优先导出公开 H5 HAR 或保存 HTML 快照。",
    },
]


def ensure_city_seed_rows(city_names: set[str]) -> None:
    rows = [row for row in iter_registry_source_slots() if not city_names or row.get("city") in city_names]
    upsert_manifest_rows(rows)


def search_duckduckgo(query_text: str) -> list[dict[str, str]]:
    search_url = "https://duckduckgo.com/html/?" + urllib.parse.urlencode({"q": query_text})
    result = fetch_url(search_url, referer="https://duckduckgo.com/")
    html_text = decode_bytes(result.get("content", b""))
    results: list[dict[str, str]] = []
    for href, label in re.findall(r'(?is)<a[^>]+class="result__a"[^>]+href="(.*?)"[^>]*>(.*?)</a>', html_text):
        href = html.unescape(href)
        parsed = urllib.parse.urlparse(href)
        if "duckduckgo.com" in parsed.netloc:
            href = urllib.parse.parse_qs(parsed.query).get("uddg", [href])[0]
        results.append(
            {
                "href": href,
                "title": normalize_whitespace(extract_visible_text(label)),
            }
        )
    return results


def candidate_allowed(channel_hint: str, candidate_url: str) -> bool:
    host = urllib.parse.urlparse(candidate_url).netloc.lower()
    if channel_hint == "wechat":
        return host == "mp.weixin.qq.com"
    return host.endswith(".gov.cn")


def title_matches(expected_title: str, candidate_title: str, body_text: str) -> bool:
    normalized_expected = normalize_whitespace(expected_title)
    normalized_candidate = normalize_whitespace(candidate_title)
    normalized_body = normalize_whitespace(body_text)
    if normalized_expected and (normalized_expected in normalized_candidate or normalized_expected in normalized_body):
        return True
    expected_tokens = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z]{2,}", normalized_expected)
    if not expected_tokens:
        return False
    hits = [token for token in expected_tokens if token in normalized_candidate or token in normalized_body]
    return len(hits) >= max(1, min(2, len(expected_tokens)))


def verify_candidate(expected_title: str, candidate_url: str) -> dict[str, str]:
    result = fetch_url(candidate_url)
    html_text = decode_bytes(result.get("content", b""))
    page_title = extract_title(html_text)
    body_text = extract_visible_text(html_text)
    verified = (
        stringify_status(result.get("http_status")) == "200"
        and "托育" in body_text
        and title_matches(expected_title, page_title, body_text)
    )
    return {
        "verified": "1" if verified else "0",
        "http_status": stringify_status(result.get("http_status")),
        "candidate_title": page_title,
        "note": result.get("blocker_reason", "") or result.get("error", "") or "",
    }


def stringify_status(value: object) -> str:
    return "" if value is None else str(value)


def extract_nanjing_entry_rows(html_text: str) -> list[dict[str, str]]:
    rows = []
    for table in extract_tables_from_html(html_text):
        if not table:
            continue
        header_text = " ".join(table[0])
        if "地区" not in header_text or "公示地址" not in header_text:
            continue
        for row in table[1:]:
            if len(row) < 3:
                continue
            rows.append(
                {
                    "district": normalize_whitespace(row[0]),
                    "site_hint": normalize_whitespace(row[1]),
                    "title": normalize_whitespace(row[2]),
                }
            )
    return rows


def build_nanjing_source_row(parent_row: dict[str, str], district: str, title: str, site_hint: str, candidate_url: str) -> dict[str, str]:
    channel = "wechat" if "微信" in site_hint or "订阅号" in site_hint else "official"
    suffix = "WX" if channel == "wechat" else "GOV"
    source_id = f"{parent_row['source_id']}_{district}_{suffix}"
    source_type = "registry_platform" if channel == "wechat" else "registry_notice"
    page_role = "district_wechat_notice" if channel == "wechat" else "district_notice"
    official_or_platform = "official-linked platform" if channel == "wechat" else "official"
    access_method = "html_parse" if candidate_url else ("manual_wechat_capture" if channel == "wechat" else "manual_browser_capture")
    source_status = "confirmed_direct" if candidate_url else "probe_pending"
    notes = f"由 {parent_row['source_id']} probe 生成；site_hint={site_hint}"
    if not candidate_url and channel == "wechat":
        notes += "；优先导出 HAR 或 HTML 快照。"
    return {
        "source_id": source_id,
        "city": parent_row.get("city", ""),
        "source_type": source_type,
        "source_name": title,
        "official_or_platform": official_or_platform,
        "url_or_page_name": candidate_url or f"{district} -> {title}",
        "target_table": parent_row.get("target_table", "nursery_registry_raw"),
        "target_fields": "institution name;address;phone;capacity;registry status",
        "access_method": access_method,
        "page_role": page_role,
        "source_status": source_status,
        "parent_source_id": parent_row.get("source_id", ""),
        "record_granularity": "institution",
        "priority": "1" if channel == "official" else "2",
        "update_date": "",
        "last_verified_date": "",
        "notes": notes,
    }


def lookup_seed_url(district: str, title: str) -> str:
    direct = NANJING_PROBE_SEEDS.get((district, title), "")
    if direct:
        return direct
    for (seed_district, seed_title), seed_url in NANJING_PROBE_SEEDS.items():
        if seed_district != district:
            continue
        if title in seed_title or seed_title in title:
            return seed_url
    return ""


def probe_nanjing() -> None:
    parent_row = get_manifest_row("NJ_REG_ENTRY_2023")
    if not parent_row:
        return

    html_text = load_html_for_source(parent_row["source_id"], RAW_OFFICIAL_DIR / "registry")
    if not html_text:
        result = fetch_url(select_source_url(parent_row))
        html_text = decode_bytes(result.get("content", b""))
    entry_rows = extract_nanjing_entry_rows(html_text)
    discovered_rows = []
    for entry in entry_rows:
        district = entry["district"]
        title = entry["title"]
        site_hint = entry["site_hint"]
        channel_hint = "wechat" if "微信" in site_hint or "订阅号" in site_hint else "official"

        seed_url = lookup_seed_url(district, title)
        candidate_url = ""
        candidate_title = ""
        if seed_url:
            verified = verify_candidate(title, seed_url)
            if verified["verified"] == "1":
                candidate_url = seed_url
                candidate_title = verified["candidate_title"]
                log_registry_probe(
                    city="南京",
                    parent_source_id=parent_row["source_id"],
                    district=district,
                    page_role="district_followdown",
                    probe_stage="seed_verify",
                    query_text=title,
                    candidate_url=seed_url,
                    candidate_title=candidate_title,
                    status="found",
                    decision="seed_verified",
                    note=site_hint,
                )
            elif verified["http_status"] != "404":
                candidate_url = seed_url
                candidate_title = verified["candidate_title"]
                log_registry_probe(
                    city="南京",
                    parent_source_id=parent_row["source_id"],
                    district=district,
                    page_role="district_followdown",
                    probe_stage="seed_verify",
                    query_text=title,
                    candidate_url=seed_url,
                    candidate_title=candidate_title,
                    status="candidate",
                    decision="seed_kept_as_candidate",
                    note=verified["note"] or site_hint,
                )

        if not candidate_url:
            if channel_hint == "wechat":
                query_text = f'site:mp.weixin.qq.com "{title}" "{district}" 南京'
            else:
                query_text = f'site:gov.cn "{title}" "{district}" 南京'
            log_registry_probe(
                city="南京",
                parent_source_id=parent_row["source_id"],
                district=district,
                page_role="district_followdown",
                probe_stage="search",
                query_text=query_text,
                status="started",
                note=site_hint,
            )
            for candidate in search_duckduckgo(query_text):
                if not candidate_allowed(channel_hint, candidate["href"]):
                    continue
                verified = verify_candidate(title, candidate["href"])
                log_registry_probe(
                    city="南京",
                    parent_source_id=parent_row["source_id"],
                    district=district,
                    page_role="district_followdown",
                    probe_stage="verify",
                    query_text=query_text,
                    candidate_url=candidate["href"],
                    candidate_title=verified["candidate_title"] or candidate["title"],
                    status="verified" if verified["verified"] == "1" else "rejected",
                    decision="keep" if verified["verified"] == "1" else "skip",
                    note=verified["note"] or site_hint,
                )
                if verified["verified"] == "1":
                    candidate_url = candidate["href"]
                    candidate_title = verified["candidate_title"] or candidate["title"]
                    break

        source_row = build_nanjing_source_row(parent_row, district, title, site_hint, candidate_url)
        discovered_rows.append(source_row)
        if candidate_url:
            log_registry_probe(
                city="南京",
                parent_source_id=parent_row["source_id"],
                source_id=source_row["source_id"],
                district=district,
                page_role=source_row["page_role"],
                probe_stage="manifest_upsert",
                candidate_url=candidate_url,
                candidate_title=candidate_title or title,
                status="found",
                decision="upsert_manifest",
                note=site_hint,
            )
            continue

        log_registry_probe(
            city="南京",
            parent_source_id=parent_row["source_id"],
            source_id=source_row["source_id"],
            district=district,
            page_role=source_row["page_role"],
            probe_stage="manifest_upsert",
            query_text=title,
            status="unresolved",
            decision="seed_manual_capture",
            note=site_hint,
        )
        if channel_hint == "wechat":
            seed_manual_capture_row(
                source_row,
                "probe_unresolved_wechat_notice",
                district=district,
                source_page=f"{district} -> {title}",
                evidence_type="har",
                evidence_title=title,
                capture_mode="browser_export",
                access_channel="wechat_h5",
                public_access_confirmed="1",
                parser_hint="wechat_public_capture",
            )
        else:
            seed_manual_capture_row(
                source_row,
                "probe_unresolved_official_notice",
                district=district,
                source_page=f"{district} -> {title}",
                evidence_type="html_snapshot",
                evidence_title=title,
                capture_mode="browser_export",
                access_channel="official_site",
                public_access_confirmed="1",
                parser_hint="official_notice_probe",
            )

    upsert_manifest_rows(discovered_rows)


def probe_suzhou() -> None:
    parent_ids = {"SZ_REG_MAP_2024", "SZ_REG_MAP_2025_NEWS", "SZ_REG_MAP_2025_WJW"}
    parent_rows = [get_manifest_row(source_id) for source_id in parent_ids]
    parent_rows = [row for row in parent_rows if row]

    discovered_rows = []
    for row in parent_rows:
        html_text = load_html_for_source(row["source_id"], RAW_OFFICIAL_DIR / "registry")
        if not html_text:
            result = fetch_url(select_source_url(row))
            html_text = decode_bytes(result.get("content", b""))
        text = extract_visible_text(html_text)
        links = extract_links(html_text, select_source_url(row))
        matched_links = [
            link
            for link in links
            if any(keyword in f"{link['href']} {link['label']}" for keyword in ("托育", "地图", "苏周到", "掌上行", "公众号"))
        ]
        log_registry_probe(
            city="苏州",
            parent_source_id=row["source_id"],
            source_id=row["source_id"],
            page_role=row.get("page_role", ""),
            probe_stage="news_probe",
            candidate_url=select_source_url(row),
            candidate_title=extract_title(html_text),
            status="scanned",
            decision="inspect_links",
            note=f"matched_links={len(matched_links)}",
        )
        if "健康苏州掌上行" in text or "苏周到" in text or "苏州健康" in text or "托育地图" in text:
            log_registry_probe(
                city="苏州",
                parent_source_id=row["source_id"],
                source_id=row["source_id"],
                page_role=row.get("page_role", ""),
                probe_stage="news_probe",
                candidate_url=select_source_url(row),
                status="hint_detected",
                decision="seed_h5_targets",
                note="官方新闻页确认 H5/公众号/APP 入口存在。",
            )

    for seed in SUZHOU_H5_SEEDS:
        manifest_row = {
            "city": "苏州",
            "source_type": "registry_platform",
            "target_table": "nursery_registry_raw",
            "target_fields": "institution name;address;phone;capacity;service scope",
            "parent_source_id": "SZ_REG_MAP_2024",
            "record_granularity": "institution",
            "priority": "1",
            "update_date": "",
            "last_verified_date": "",
            **seed,
        }
        discovered_rows.append(manifest_row)
        log_registry_probe(
            city="苏州",
            parent_source_id="SZ_REG_MAP_2024",
            source_id=seed["source_id"],
            page_role=seed["page_role"],
            probe_stage="seed",
            query_text=seed["source_name"],
            status="seeded",
            decision="upsert_manifest",
            note=seed["notes"],
        )
        seed_manual_capture_row(
            manifest_row,
            "suzhou_public_h5_probe",
            evidence_type="har",
            evidence_title=seed["source_name"],
            capture_mode="browser_export",
            access_channel="wechat_h5" if "wechat" in seed["page_role"] else "app_webview",
            public_access_confirmed="1",
            parser_hint="public_h5_probe",
        )

    upsert_manifest_rows(discovered_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe registry entry pages and seed derived official sources.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    args = parser.parse_args()

    selected_cities = set(args.city)
    ensure_city_seed_rows(selected_cities)

    if not selected_cities or "南京" in selected_cities:
        probe_nanjing()
    if not selected_cities or "苏州" in selected_cities:
        probe_suzhou()

    print("registry_probe complete")


if __name__ == "__main__":
    main()

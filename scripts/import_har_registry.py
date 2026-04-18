from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
import urllib.parse

from normalize_registry_evidence import (
    build_derived_source_id,
    extract_rows_from_html_tables,
    extract_rows_from_json_payload,
    materialize_derived_source,
)
from pipeline_common import (
    DOCS_DIR,
    RAW_OFFICIAL_DIR,
    file_sha1,
    get_manifest_row,
    save_text,
    schema_fieldnames,
    upsert_manifest_rows,
    write_csv_rows,
    read_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")
EXPECTED_ENDPOINT = "/daycare/szMenuOption/getHomeList"


def resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return DOCS_DIR.parent / path


def should_process(row: dict[str, str], cities: set[str], source_ids: set[str], manual_ids: set[str]) -> bool:
    if cities and row.get("city") not in cities:
        return False
    if source_ids and row.get("source_id") not in source_ids:
        return False
    if manual_ids and row.get("manual_id") not in manual_ids:
        return False
    evidence_type = row.get("evidence_type", "").strip().lower()
    if evidence_type != "har" and row.get("capture_status", "").upper() != "HAR_EXPORTED":
        return False
    return bool(row.get("evidence_file_path", "").strip())


def decode_har_content(content: dict[str, object]) -> str:
    text = content.get("text", "")
    if not isinstance(text, str):
        return ""
    if content.get("encoding") == "base64":
        try:
            return base64.b64decode(text).decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return text


def append_reason(row: dict[str, str], reason: str) -> None:
    reason = reason.strip()
    if not reason:
        return
    parts = [part.strip() for part in row.get("remark", "").split("|") if part.strip()]
    if reason not in parts:
        parts.append(reason)
    row["remark"] = " | ".join(parts)


def response_size(content: dict[str, object]) -> int:
    try:
        return int(content.get("size", 0) or 0)
    except (TypeError, ValueError):
        return 0


def build_candidate_urls(candidates: list[str], expected_present: bool) -> list[str]:
    ordered = []
    if not expected_present:
        ordered.append(f"missing_expected_endpoint={EXPECTED_ENDPOINT}")
    ordered.extend(candidates[:8])
    return ordered


def update_manual_rows(updated_rows: list[dict[str, str]]) -> None:
    write_csv_rows(DOCS_DIR / "manual_capture_template.csv", OUTPUT_FIELDS, updated_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import HAR evidence into derived registry sources.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    parser.add_argument("--source-id", nargs="*", default=[], help="Limit to source IDs")
    parser.add_argument("--manual-id", nargs="*", default=[], help="Limit to manual IDs")
    args = parser.parse_args()

    manual_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    imported = 0

    for row in manual_rows:
        if not should_process(row, set(args.city), set(args.source_id), set(args.manual_id)):
            continue
        source_row = get_manifest_row(row.get("source_id", "")) or {
            "source_id": row.get("source_id", ""),
            "city": row.get("city", ""),
            "target_table": "nursery_registry_raw",
            "official_or_platform": "official-linked platform",
            "priority": "1",
        }
        evidence_path = resolve_path(row.get("evidence_file_path", ""))
        if not evidence_path.exists():
            row["import_status"] = "FAILED"
            row["remark"] = f"{row.get('remark', '')} | missing_file:{evidence_path}".strip(" |")
            continue

        evidence_sha1 = file_sha1(evidence_path)
        derived_source_id = row.get("derived_source_id", "") or build_derived_source_id(
            row.get("source_id", ""),
            f"{evidence_path}|{evidence_sha1}",
            "HAR",
        )
        har_payload = json.loads(evidence_path.read_text(encoding="utf-8", errors="ignore"))
        entries = har_payload.get("log", {}).get("entries", [])
        normalized_rows = []
        largest_html = ""
        largest_html_url = ""
        evidence_host = urllib.parse.urlparse(row.get("evidence_url_final", "")).netloc
        candidate_json_urls: list[str] = []
        expected_endpoint_present = False
        incomplete_payload_detected = False
        for entry in entries:
            request = entry.get("request", {})
            response = entry.get("response", {})
            request_url = str(request.get("url", ""))
            status = str(response.get("status", ""))
            content = response.get("content", {})
            mime_type = str(content.get("mimeType", "")).lower()
            if not request_url.startswith("http") or status != "200":
                continue
            request_host = urllib.parse.urlparse(request_url).netloc
            same_origin = not evidence_host or request_host == evidence_host
            is_candidate_json = same_origin and ("json" in mime_type) and any(marker in request_url for marker in ("/daycare/", "/jeecg-boot/"))
            if is_candidate_json and request_url not in candidate_json_urls:
                candidate_json_urls.append(request_url)
            if EXPECTED_ENDPOINT in request_url:
                expected_endpoint_present = True
            raw_text = content.get("text")
            if is_candidate_json and response_size(content) > 0 and (not isinstance(raw_text, str) or not raw_text.strip()):
                incomplete_payload_detected = True
            content_text = decode_har_content(content)
            if not content_text:
                continue

            if "json" in mime_type or content_text.lstrip().startswith(("{", "[")):
                try:
                    payload = json.loads(content_text)
                except json.JSONDecodeError:
                    payload = None
                if payload is not None:
                    normalized_rows.extend(extract_rows_from_json_payload(payload, default_district=row.get("district", "")))
                    continue

            if "html" in mime_type or "<html" in content_text.lower():
                rows = extract_rows_from_html_tables(content_text, default_district=row.get("district", ""))
                normalized_rows.extend(rows)
                if len(content_text) > len(largest_html):
                    largest_html = content_text
                    largest_html_url = request_url

        if normalized_rows:
            title = row.get("evidence_title", "") or f"{row.get('source_id', '')} HAR 导入"
            materialize_derived_source(
                parent_source_row=source_row,
                derived_source_id=derived_source_id,
                title=title,
                rows=normalized_rows,
                source_url=row.get("evidence_url_final", "") or largest_html_url or row.get("source_page", ""),
                source_type="registry_har_payload",
                access_method="har_import",
                page_role="har_payload",
                notes=f"har_sha1={evidence_sha1}",
            )
            row["import_status"] = "NORMALIZED"
        elif largest_html:
            target_path = RAW_OFFICIAL_DIR / "registry" / f"{derived_source_id}.html"
            save_text(target_path, largest_html)
            upsert_manifest_rows(
                [
                    {
                        "source_id": derived_source_id,
                        "city": source_row.get("city", ""),
                        "source_type": "registry_har_html",
                        "source_name": row.get("evidence_title", "") or f"{row.get('source_id', '')} HAR HTML",
                        "official_or_platform": source_row.get("official_or_platform", "official-linked platform"),
                        "url_or_page_name": row.get("evidence_url_final", "") or largest_html_url or row.get("source_page", ""),
                        "target_table": "nursery_registry_raw",
                        "target_fields": "institution name;address;phone;capacity;registry status",
                        "access_method": "har_import",
                        "page_role": "har_html_snapshot",
                        "source_status": "confirmed_direct",
                        "parent_source_id": row.get("source_id", ""),
                        "record_granularity": "institution",
                        "priority": source_row.get("priority", "1"),
                        "update_date": "",
                        "last_verified_date": "",
                        "notes": f"raw_har_html_sha1={evidence_sha1}",
                    }
                ]
            )
            row["import_status"] = "IMPORTED"
        else:
            row["import_status"] = "FAILED"
            if incomplete_payload_detected:
                append_reason(row, "incomplete_har_payload")
            else:
                append_reason(row, "no_registry_payload_in_har")
            for candidate in build_candidate_urls(candidate_json_urls, expected_endpoint_present):
                append_reason(row, candidate)
            continue

        row["derived_source_id"] = derived_source_id
        row["evidence_file_sha1"] = evidence_sha1
        imported += 1

    update_manual_rows(manual_rows)
    print(f"import_har_registry complete: imported={imported}")


if __name__ == "__main__":
    main()

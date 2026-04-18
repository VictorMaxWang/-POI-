from __future__ import annotations

import argparse
import re
import shutil
from datetime import datetime
from pathlib import Path

from pipeline_common import (
    DOCS_DIR,
    PROJECT_ROOT,
    RAW_OFFICIAL_DIR,
    file_sha1,
    get_manifest_row,
    make_hash_id,
    read_csv_rows,
    schema_fieldnames,
    write_csv_rows,
)


OUTPUT_FIELDS = schema_fieldnames("manual_capture_template.csv")
CAPTURE_STATUS_MAP = {
    "json_response_export": "JSON_EXPORTED",
    "har": "HAR_EXPORTED",
    "html_snapshot": "HTML_SAVED",
}
DEFAULT_SUFFIX_MAP = {
    "json_response_export": ".json",
    "har": ".har",
    "html_snapshot": ".html",
}


def ascii_slug(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", text.strip().lower())
    value = value.strip("_")
    return value or "item"


def resolve_input_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def infer_city(source_id: str, explicit_city: str, manual_rows: list[dict[str, str]]) -> str:
    if explicit_city:
        return explicit_city
    manifest_row = get_manifest_row(source_id)
    if manifest_row.get("city"):
        return manifest_row["city"]
    for row in manual_rows:
        if row.get("source_id") == source_id and row.get("city"):
            return row["city"]
    return ""


def score_context_row(row: dict[str, str], source_id: str, city: str) -> int:
    score = 0
    if row.get("source_id") == source_id:
        score += 100
    if city and row.get("city") == city:
        score += 40
    if row.get("page_role") == "platform_list":
        score += 30
    if row.get("page_role"):
        score += 10
    if row.get("source_page"):
        score += 5
    if row.get("capture_mode"):
        score += 5
    if row.get("access_channel"):
        score += 5
    if "健康苏州掌上行" in row.get("source_page", ""):
        score += 20
    return score


def choose_context_row(manual_rows: list[dict[str, str]], source_id: str, city: str) -> dict[str, str]:
    candidates = [row for row in manual_rows if row.get("source_id") == source_id]
    if city:
        city_candidates = [row for row in candidates if row.get("city") == city]
        if city_candidates:
            candidates = city_candidates
    if not candidates:
        return {}
    return max(candidates, key=lambda row: score_context_row(row, source_id, city))


def base_row_from_context(context_row: dict[str, str], source_id: str, city: str) -> dict[str, str]:
    manifest_row = get_manifest_row(source_id)
    base = {field: "" for field in OUTPUT_FIELDS}
    base["task_batch"] = context_row.get("task_batch", "") or f"{city or manifest_row.get('city', 'ALL')}_REGISTRY_BOOTSTRAP"
    base["city"] = city or context_row.get("city", "") or manifest_row.get("city", "")
    base["district"] = context_row.get("district", "")
    base["source_id"] = source_id
    base["page_role"] = context_row.get("page_role", "") or manifest_row.get("page_role", "") or "platform_list"
    base["parent_source_id"] = context_row.get("parent_source_id", "") or manifest_row.get("parent_source_id", "")
    base["source_page"] = context_row.get("source_page", "") or manifest_row.get("url_or_page_name", "") or manifest_row.get("source_name", "") or source_id
    base["capture_mode"] = context_row.get("capture_mode", "")
    base["access_channel"] = context_row.get("access_channel", "")
    base["public_access_confirmed"] = context_row.get("public_access_confirmed", "") or "1"
    base["parser_hint"] = context_row.get("parser_hint", "")
    base["remark"] = context_row.get("remark", "")
    return base


def registered_copy_path(source_id: str, evidence_type: str, source_path: Path, sha1_value: str) -> Path:
    suffix = source_path.suffix.lower() or DEFAULT_SUFFIX_MAP.get(evidence_type, ".bin")
    filename = f"{ascii_slug(source_id)}_{ascii_slug(evidence_type)}_{sha1_value[:8]}{suffix}"
    return RAW_OFFICIAL_DIR / "registry_evidence" / "registered" / filename


def relative_project_path(path: Path) -> str:
    return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()


def choose_existing_row_index(
    rows: list[dict[str, str]],
    *,
    source_id: str,
    evidence_type: str,
    source_page: str,
    source_input_path: Path,
    dest_rel_path: str,
) -> int | None:
    for index, row in enumerate(rows):
        if row.get("source_id") != source_id or row.get("evidence_type", "").strip().lower() != evidence_type:
            continue
        existing_path = row.get("evidence_file_path", "").strip()
        if not existing_path:
            continue
        existing_resolved = resolve_input_path(existing_path)
        if existing_resolved.exists():
            try:
                if existing_resolved.resolve() == source_input_path.resolve() or existing_resolved.resolve() == resolve_input_path(dest_rel_path).resolve():
                    return index
            except FileNotFoundError:
                pass
    for index, row in enumerate(rows):
        if (
            row.get("source_id") == source_id
            and row.get("evidence_type", "").strip().lower() == evidence_type
            and row.get("source_page", "") == source_page
        ):
            return index
    return None


def build_manual_row(
    *,
    base_row: dict[str, str],
    source_id: str,
    evidence_type: str,
    title: str,
    url: str,
    relative_path: str,
    sha1_value: str,
    captured_at: str,
    existing_manual_id: str = "",
) -> dict[str, str]:
    row = {field: base_row.get(field, "") for field in OUTPUT_FIELDS}
    row["manual_id"] = existing_manual_id or make_hash_id("manual", source_id, evidence_type, relative_path, url, title)
    row["capture_status"] = CAPTURE_STATUS_MAP.get(evidence_type, "TODO")
    row["evidence_type"] = evidence_type
    row["evidence_title"] = title
    row["evidence_url_final"] = url
    row["evidence_file_path"] = relative_path
    row["evidence_file_sha1"] = sha1_value
    row["captured_at"] = captured_at
    row["import_status"] = "PENDING"
    row["derived_source_id"] = ""
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description="Register a registry evidence file into manual_capture_template.csv.")
    parser.add_argument("--source-id", required=True, help="Registry source_id")
    parser.add_argument("--evidence-type", required=True, choices=sorted(CAPTURE_STATUS_MAP), help="Evidence type")
    parser.add_argument("--file", required=True, help="Path to the evidence file")
    parser.add_argument("--city", default="", help="Optional city override")
    parser.add_argument("--title", default="", help="Optional evidence title")
    parser.add_argument("--url", default="", help="Optional final evidence URL")
    args = parser.parse_args()

    manual_rows = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    source_path = resolve_input_path(args.file)
    if not source_path.exists():
        raise SystemExit(f"Evidence file not found: {source_path}")

    sha1_value = file_sha1(source_path)
    captured_at = datetime.fromtimestamp(source_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    city = infer_city(args.source_id, args.city, manual_rows)
    context_row = choose_context_row(manual_rows, args.source_id, city)
    base_row = base_row_from_context(context_row, args.source_id, city)

    dest_path = registered_copy_path(args.source_id, args.evidence_type, source_path, sha1_value)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.resolve() != dest_path.resolve():
        shutil.copy2(source_path, dest_path)

    relative_path = relative_project_path(dest_path)
    title = args.title or context_row.get("evidence_title", "") or f"{args.source_id} {args.evidence_type}"
    url = args.url or context_row.get("evidence_url_final", "") or get_manifest_row(args.source_id).get("url_or_page_name", "")
    row_index = choose_existing_row_index(
        manual_rows,
        source_id=args.source_id,
        evidence_type=args.evidence_type,
        source_page=base_row.get("source_page", ""),
        source_input_path=source_path,
        dest_rel_path=relative_path,
    )
    existing_manual_id = manual_rows[row_index].get("manual_id", "") if row_index is not None else ""
    new_row = build_manual_row(
        base_row=base_row,
        source_id=args.source_id,
        evidence_type=args.evidence_type,
        title=title,
        url=url,
        relative_path=relative_path,
        sha1_value=sha1_value,
        captured_at=captured_at,
        existing_manual_id=existing_manual_id,
    )

    if row_index is None:
        manual_rows.append(new_row)
    else:
        manual_rows[row_index] = new_row

    write_csv_rows(DOCS_DIR / "manual_capture_template.csv", OUTPUT_FIELDS, manual_rows)
    print(f"manual_id={new_row['manual_id']}")
    print(f"evidence_file_path={relative_path}")


if __name__ == "__main__":
    main()

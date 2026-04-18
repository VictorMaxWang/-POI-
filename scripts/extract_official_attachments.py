from __future__ import annotations

import argparse
import csv
import io
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

from normalize_registry_evidence import (
    build_derived_source_id,
    is_plausible_registry_row,
    materialize_derived_source,
    match_field,
    normalize_record,
)
from pipeline_common import (
    RAW_OFFICIAL_DIR,
    decode_bytes,
    extract_links,
    extract_title,
    fetch_url,
    file_sha1,
    load_html_for_source,
    load_manifest,
    normalize_whitespace,
    save_response,
    select_source_url,
)

try:
    import openpyxl  # type: ignore
except ImportError:  # pragma: no cover
    openpyxl = None

try:
    from pypdf import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None


ATTACHMENT_SUFFIXES = {".docx", ".doc", ".xlsx", ".xls", ".csv", ".pdf"}
WORD_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def select_best_header_row(table: list[list[str]]) -> tuple[dict[str, int], int]:
    best_indexes: dict[str, int] = {}
    best_start = 1
    for header_idx in range(min(2, len(table))):
        indexes: dict[str, int] = {}
        for cell_idx, cell in enumerate(table[header_idx]):
            field_name = match_field(cell)
            if field_name and field_name not in indexes:
                indexes[field_name] = cell_idx
        if len(indexes) > len(best_indexes):
            best_indexes = indexes
            best_start = header_idx + 1
    return best_indexes, best_start


def records_from_table(table: list[list[str]], default_district: str = "") -> list[dict[str, str]]:
    indexes, start_row = select_best_header_row(table)
    if "institution_name_raw" not in indexes:
        return []
    output = []
    for row in table[start_row:]:
        record = {field_name: row[idx] for field_name, idx in indexes.items() if idx < len(row)}
        normalized = normalize_record(record, default_district=default_district)
        if is_plausible_registry_row(normalized):
            output.append(normalized)
    return output


def extract_attachment_links_for_source(source_row: dict[str, str], html_text: str) -> list[str]:
    output = []
    for link in extract_links(html_text, select_source_url(source_row)):
        suffix = Path(urllib.parse.urlparse(link["href"]).path).suffix.lower()
        if suffix in ATTACHMENT_SUFFIXES:
            output.append(link["href"])
    deduped = []
    seen = set()
    for url in output:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def parse_docx_tables(path: Path) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    with zipfile.ZipFile(path) as archive:
        document_xml = archive.read("word/document.xml")
    root = ET.fromstring(document_xml)
    for table_node in root.findall(".//w:tbl", WORD_NS):
        rows: list[list[str]] = []
        for row_node in table_node.findall("./w:tr", WORD_NS):
            row_values: list[str] = []
            for cell_node in row_node.findall("./w:tc", WORD_NS):
                texts = [normalize_whitespace(text_node.text or "") for text_node in cell_node.findall(".//w:t", WORD_NS)]
                row_values.append(normalize_whitespace(" ".join(filter(None, texts))))
            if any(row_values):
                rows.append(row_values)
        if rows:
            tables.append(rows)
    return tables


def parse_csv_rows(path: Path) -> list[list[list[str]]]:
    content = path.read_text(encoding="utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(content))
    rows = [[normalize_whitespace(cell) for cell in row] for row in reader if any(normalize_whitespace(cell) for cell in row)]
    return [rows] if rows else []


def parse_xlsx_rows(path: Path) -> list[list[list[str]]]:
    if openpyxl is None:
        return []
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    tables = []
    for worksheet in workbook.worksheets:
        rows = []
        for row in worksheet.iter_rows(values_only=True):
            values = [normalize_whitespace("" if value is None else str(value)) for value in row]
            if any(values):
                rows.append(values)
        if rows:
            tables.append(rows)
    return tables


def parse_pdf_rows(path: Path) -> list[list[list[str]]]:
    if PdfReader is None:
        return []
    reader = PdfReader(str(path))
    lines = []
    for page in reader.pages:
        text = page.extract_text() or ""
        lines.extend(normalize_whitespace(line) for line in text.splitlines() if normalize_whitespace(line))
    return [[[line] for line in lines]] if lines else []


def parse_attachment_records(path: Path, default_district: str = "") -> list[dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".docx":
        tables = parse_docx_tables(path)
    elif suffix == ".csv":
        tables = parse_csv_rows(path)
    elif suffix in {".xlsx", ".xls"}:
        tables = parse_xlsx_rows(path)
    elif suffix == ".pdf":
        tables = parse_pdf_rows(path)
    else:
        tables = []

    output = []
    for table in tables:
        output.extend(records_from_table(table, default_district=default_district))
    return output


def iter_registry_sources(cities: set[str], source_ids: set[str]) -> list[dict[str, str]]:
    rows = [row for row in load_manifest() if row.get("target_table") == "nursery_registry_raw"]
    if cities:
        rows = [row for row in rows if row.get("city") in cities]
    if source_ids:
        rows = [row for row in rows if row.get("source_id") in source_ids]
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and normalize official registry attachments.")
    parser.add_argument("--city", nargs="*", default=[], help="Limit to one or more cities")
    parser.add_argument("--source-id", nargs="*", default=[], help="Limit to source IDs")
    args = parser.parse_args()

    attachment_dir = RAW_OFFICIAL_DIR / "registry_evidence" / "attachments"
    sources = iter_registry_sources(set(args.city), set(args.source_id))
    created = 0
    downloaded = 0

    for source_row in sources:
        html_text = load_html_for_source(source_row["source_id"], RAW_OFFICIAL_DIR / "registry")
        if not html_text:
            continue
        attachment_urls = extract_attachment_links_for_source(source_row, html_text)
        for idx, attachment_url in enumerate(attachment_urls, start=1):
            result = fetch_url(attachment_url, referer=select_source_url(source_row))
            if stringify_status(result.get("http_status")) != "200":
                continue
            suffix = Path(urllib.parse.urlparse(attachment_url).path).suffix.lower() or ".bin"
            attachment_id = f"{source_row['source_id']}_ATTACH_{idx}"
            local_path = save_response(result["content"], attachment_dir, attachment_id, suffix)
            downloaded += 1
            records = parse_attachment_records(local_path, default_district=source_row.get("district", ""))
            if not records:
                continue
            derived_source_id = build_derived_source_id(source_row["source_id"], f"{attachment_url}|{file_sha1(local_path)}", "ATT")
            title = extract_title(html_text) or source_row.get("source_name", "")
            materialize_derived_source(
                parent_source_row=source_row,
                derived_source_id=derived_source_id,
                title=f"{title} 附件解析",
                rows=records,
                source_url=attachment_url,
                source_type="registry_attachment_table",
                access_method="normalized_attachment",
                page_role="attachment_table",
                notes=f"attachment_sha1={file_sha1(local_path)}; extracted_from={source_row['source_id']}",
            )
            created += 1

    print(f"extract_official_attachments complete: downloaded={downloaded} derived_sources={created}")


def stringify_status(value: object) -> str:
    return "" if value is None else str(value)


if __name__ == "__main__":
    main()

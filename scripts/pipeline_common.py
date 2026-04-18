from __future__ import annotations

import csv
import hashlib
import html
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Iterable

from pipeline_schema import (
    FETCH_LOG_FIELDS,
    MANUAL_CAPTURE_FIELDS,
    MANUAL_REVIEW_FIELDS,
    SOURCE_MANIFEST_FIELDS,
    TABLE_SCHEMAS,
    TEXT_TAG_RULES,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_OFFICIAL_DIR = PROJECT_ROOT / "raw_official"
RAW_API_DIR = PROJECT_ROOT / "raw_api"
CLEAN_DIR = PROJECT_ROOT / "clean"
TEXT_DIR = PROJECT_ROOT / "text"
LOGS_DIR = PROJECT_ROOT / "logs"
DOCS_DIR = PROJECT_ROOT / "docs"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

BLOCK_MARKERS = [
    "Knownsec CloudWAF",
    "request has been blocked",
    "wafblock",
    "验证码",
    "访问异常",
    "禁止访问",
    "verify you are human",
]

BLOCKER_SECTION_ORDER = ["苏州", "南通", "南京", "盐城", "ALL", "未分类"]


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: stringify(row.get(name, "")) for name in fieldnames})


def append_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({name: stringify(row.get(name, "")) for name in fieldnames})


def ensure_csv(path: Path, fieldnames: list[str]) -> None:
    if path.exists():
        return
    write_csv_rows(path, fieldnames, [])


def stringify(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def load_manifest() -> list[dict[str, str]]:
    manifest_path = DOCS_DIR / "source_manifest.csv"
    ensure_csv(manifest_path, SOURCE_MANIFEST_FIELDS)
    return read_csv_rows(manifest_path)


def schema_fieldnames(filename: str) -> list[str]:
    return [field for field, _, _, _ in TABLE_SCHEMAS[filename]]


def ensure_standard_files() -> None:
    ensure_csv(LOGS_DIR / "fetch_log.csv", FETCH_LOG_FIELDS)
    ensure_csv(LOGS_DIR / "manual_review_list.csv", MANUAL_REVIEW_FIELDS)
    ensure_csv(DOCS_DIR / "manual_capture_template.csv", MANUAL_CAPTURE_FIELDS)


def make_hash_id(prefix: str, *parts: object) -> str:
    joined = "||".join(normalize_whitespace(stringify(part)) for part in parts)
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def slugify(text_value: str) -> str:
    value = re.sub(r"[^\w.-]+", "_", text_value.strip(), flags=re.UNICODE)
    value = value.strip("._")
    return value or "item"


def normalize_whitespace(text_value: str) -> str:
    text_value = stringify(text_value).replace("\u3000", " ")
    return re.sub(r"\s+", " ", text_value).strip()


def normalize_name(name: str) -> str:
    text_value = normalize_whitespace(name)
    return (
        text_value.replace("（", "(")
        .replace("）", ")")
        .replace("【", "[")
        .replace("】", "]")
    )


def normalize_address(address: str) -> str:
    text_value = normalize_whitespace(address)
    return (
        text_value.replace("（", "(")
        .replace("）", ")")
        .replace("，", ",")
        .replace("；", ";")
    )


def normalize_flag(value: str) -> str:
    text_value = normalize_whitespace(value)
    if not text_value:
        return ""
    lowered = text_value.lower()
    if lowered in {"1", "true", "yes", "y", "是", "有"}:
        return "1"
    if lowered in {"0", "false", "no", "n", "否", "无"}:
        return "0"
    if any(keyword in text_value for keyword in ("普惠", "示范", "社区", "连锁", "备案")):
        return "1"
    return text_value


def extract_visible_text(html_text: str) -> str:
    html_text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    html_text = re.sub(r"(?is)<style.*?>.*?</style>", " ", html_text)
    text_value = re.sub(r"(?is)<br\s*/?>", "\n", html_text)
    text_value = re.sub(r"(?is)</p>", "\n", text_value)
    text_value = re.sub(r"(?is)<[^>]+>", " ", text_value)
    text_value = html.unescape(text_value)
    text_value = text_value.replace("\r", "\n")
    text_value = re.sub(r"\n{3,}", "\n\n", text_value)
    text_value = re.sub(r"[ \t]+", " ", text_value)
    return text_value.strip()


def extract_title(html_text: str) -> str:
    patterns = (
        r"(?is)<title>(.*?)</title>",
        r'(?is)<meta[^>]+ArticleTitle"[^>]+content="(.*?)"',
    )
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            return normalize_whitespace(html.unescape(match.group(1)))
    return ""


def extract_links(html_text: str, base_url: str) -> list[dict[str, str]]:
    links = []
    for href, label in re.findall(r'(?is)<a[^>]+href=["\'](.*?)["\'][^>]*>(.*?)</a>', html_text):
        absolute = urllib.parse.urljoin(base_url, html.unescape(href.strip()))
        label_text = normalize_whitespace(extract_visible_text(label))
        links.append({"href": absolute, "label": label_text})
    return links


def extract_tables_from_html(html_text: str) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table_html in re.findall(r"(?is)<table[^>]*>(.*?)</table>", html_text):
        rows: list[list[str]] = []
        for row_html in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", table_html):
            cells = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", row_html)
            parsed_cells = [normalize_whitespace(extract_visible_text(cell)) for cell in cells]
            if parsed_cells:
                rows.append(parsed_cells)
        if rows:
            tables.append(rows)
    return tables


def detect_blocker(content_text: str, http_status: str | int) -> str:
    lowered = content_text.lower()
    for marker in BLOCK_MARKERS:
        if marker.lower() in lowered:
            return marker
    if stringify(http_status).startswith("4"):
        return f"http_{http_status}"
    return ""


def decode_bytes(content: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "gbk", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


def fetch_url(url: str, referer: str | None = None, timeout: int = 30) -> dict[str, object]:
    headers = dict(DEFAULT_HEADERS)
    if referer:
        headers["Referer"] = referer

    def _single_fetch(verify_ssl: bool) -> dict[str, object]:
        request = urllib.request.Request(url, headers=headers)
        context = ssl.create_default_context()
        if not verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
                content = response.read()
                return {
                    "http_status": response.getcode(),
                    "content_type": response.headers.get("Content-Type", ""),
                    "content": content,
                    "fetch_mode": "normal_tls" if verify_ssl else "insecure_tls_retry",
                    "error": "",
                }
        except urllib.error.HTTPError as exc:
            content = exc.read()
            return {
                "http_status": exc.code,
                "content_type": exc.headers.get("Content-Type", "") if exc.headers else "",
                "content": content,
                "fetch_mode": "normal_tls" if verify_ssl else "insecure_tls_retry",
                "error": stringify(exc),
            }

    try:
        result = _single_fetch(True)
    except ssl.SSLError as exc:
        result = _single_fetch(False)
        result["error"] = stringify(exc)
    except urllib.error.URLError as exc:
        if "CERTIFICATE_VERIFY_FAILED" in stringify(exc):
            result = _single_fetch(False)
            result["error"] = stringify(exc)
        else:
            return {
                "http_status": "",
                "content_type": "",
                "content": b"",
                "fetch_mode": "normal_tls",
                "error": stringify(exc),
                "blocker_reason": stringify(exc),
            }

    result["blocker_reason"] = detect_blocker(decode_bytes(result["content"]), result["http_status"])
    return result


def guess_extension(content_type: str, url: str) -> str:
    lowered = (content_type or "").lower()
    if "pdf" in lowered or url.lower().endswith(".pdf"):
        return ".pdf"
    if "json" in lowered or url.lower().endswith(".json"):
        return ".json"
    if "html" in lowered or ".shtml" in url.lower() or ".html" in url.lower():
        return ".html"
    if "text" in lowered:
        return ".txt"
    return Path(urllib.parse.urlparse(url).path).suffix or ".bin"


def save_response(content: bytes, target_dir: Path, source_id: str, extension: str) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{slugify(source_id)}{extension}"
    path.write_bytes(content)
    return path


def log_fetch(stage: str, source_row: dict[str, str], result: dict[str, object], local_path: Path | None, note: str = "") -> None:
    ensure_standard_files()
    append_csv_rows(
        LOGS_DIR / "fetch_log.csv",
        FETCH_LOG_FIELDS,
        [
            {
                "fetch_time": now_ts(),
                "stage": stage,
                "source_id": source_row.get("source_id", ""),
                "city": source_row.get("city", ""),
                "target_table": source_row.get("target_table", ""),
                "source_url": source_row.get("url_or_page_name", ""),
                "http_status": result.get("http_status", ""),
                "fetch_mode": result.get("fetch_mode", ""),
                "blocker_flag": "1" if result.get("blocker_reason") else "0",
                "local_path": str(local_path) if local_path else "",
                "content_type": result.get("content_type", ""),
                "note": note or result.get("error", ""),
            }
        ],
    )


def ensure_blockers_file() -> Path:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    blockers_path = LOGS_DIR / "blockers.md"
    if blockers_path.exists():
        return blockers_path

    lines = [
        "# blockers",
        "",
        "记录无法稳定自动采集的来源、阻塞原因和人工补录建议。按城市归档，便于逐城清理。",
        "",
    ]
    for city in BLOCKER_SECTION_ORDER:
        lines.extend([f"## {city}", ""])
    blockers_path.write_text("\n".join(lines), encoding="utf-8")
    return blockers_path


def insert_markdown_under_city_heading(content: str, city: str, block: str) -> str:
    heading = f"## {city}"
    if heading not in content:
        if not content.endswith("\n"):
            content += "\n"
        return content + f"\n{heading}\n\n{block}"

    start = content.index(heading)
    heading_line_end = content.find("\n", start)
    if heading_line_end == -1:
        heading_line_end = len(content)
    next_heading = content.find("\n## ", heading_line_end + 1)
    insert_at = next_heading + 1 if next_heading != -1 else len(content)
    if not block.endswith("\n"):
        block += "\n"
    return content[:insert_at] + block + content[insert_at:]


def append_blocker(stage: str, source_row: dict[str, str], reason: str, manual_action: str) -> None:
    blockers_path = ensure_blockers_file()
    city = source_row.get("city", "") or "未分类"
    block = (
        f"### {now_ts()} | {stage} | {source_row.get('source_id', '')}\n"
        f"- source_id: {source_row.get('source_id', '')}\n"
        f"- page_role: {source_row.get('page_role', '')}\n"
        f"- url: {source_row.get('url_or_page_name', '')}\n"
        f"- reason: {reason}\n"
        f"- last_seen: {now_ts()}\n"
        f"- next_action: {manual_action}\n"
        f"- resolved_flag: 0\n\n"
    )
    content = blockers_path.read_text(encoding="utf-8")
    blockers_path.write_text(insert_markdown_under_city_heading(content, city, block), encoding="utf-8")


def seed_manual_capture_row(
    source_row: dict[str, str],
    remark: str,
    *,
    district: str = "",
    source_page: str = "",
    evidence_title: str = "",
    capture_status: str = "TODO",
) -> None:
    ensure_standard_files()
    existing = read_csv_rows(DOCS_DIR / "manual_capture_template.csv")
    key = (
        source_row.get("source_id", ""),
        source_row.get("city", ""),
        district,
        source_row.get("page_role", ""),
        source_page or source_row.get("url_or_page_name", ""),
        remark,
    )
    seen = {
        (
            row.get("source_id", ""),
            row.get("city", ""),
            row.get("district", ""),
            row.get("page_role", ""),
            row.get("source_page", ""),
            row.get("remark", ""),
        )
        for row in existing
    }
    if key in seen:
        return

    append_csv_rows(
        DOCS_DIR / "manual_capture_template.csv",
        MANUAL_CAPTURE_FIELDS,
        [
            {
                "manual_id": make_hash_id("manual", source_row.get("source_id"), district, remark),
                "task_batch": f"{source_row.get('city', 'ALL')}_REGISTRY_BOOTSTRAP",
                "capture_status": capture_status,
                "city": source_row.get("city", ""),
                "district": district,
                "source_id": source_row.get("source_id", ""),
                "page_role": source_row.get("page_role", ""),
                "parent_source_id": source_row.get("parent_source_id", ""),
                "source_page": source_page or source_row.get("url_or_page_name", ""),
                "evidence_title": evidence_title or source_row.get("source_name", ""),
                "evidence_url_final": "",
                "institution_name_raw": "",
                "address_raw": "",
                "phone_raw": "",
                "operator_name_raw": "",
                "capacity_raw": "",
                "registry_status_raw": "",
                "inclusive_flag_raw": "",
                "demo_flag_raw": "",
                "capture_person": "",
                "capture_date": "",
                "screenshot_path": "",
                "remark": remark,
            }
        ],
    )


def load_html_for_source(source_id: str, folder: Path) -> str:
    for candidate in folder.glob(f"{slugify(source_id)}.*"):
        if candidate.suffix.lower() in {".html", ".htm", ".txt"}:
            return decode_bytes(candidate.read_bytes())
    return ""


def maybe_number(value: str) -> str:
    text_value = normalize_whitespace(value)
    if not text_value:
        return ""
    return text_value.replace(",", "").rstrip("%")


def extract_publish_date(html_text: str) -> str:
    patterns = [
        r'PubDate" content="([^"]+)"',
        r"发布时间[:：]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r"发布日期[:：]?\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        r'Maketime" content="([0-9]{4}-[0-9]{2}-[0-9]{2})',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text)
        if match:
            return normalize_whitespace(match.group(1))[:10]
    return ""


def select_source_url(source_row: dict[str, str]) -> str:
    return source_row.get("url_or_page_name", "")


def iter_schema_rows() -> list[dict[str, str]]:
    rows = []
    for table_name, fields in TABLE_SCHEMAS.items():
        for field_name, field_type, required, description in fields:
            rows.append(
                {
                    "table_name": table_name,
                    "field_name": field_name,
                    "field_type": field_type,
                    "required": required,
                    "description": description,
                }
            )
    return rows


def label_text(text_value: str) -> tuple[str, str]:
    text_value = normalize_whitespace(text_value)
    tags: list[str] = []
    details: list[str] = []
    for tag_name, patterns in TEXT_TAG_RULES:
        hits = [pattern for pattern in patterns if re.search(pattern, text_value, flags=re.I)]
        if hits:
            tags.append(tag_name)
            details.append(f"{tag_name}:{'|'.join(hits)}")
    return "|".join(tags), "; ".join(details)


def fetch_json(url: str, timeout: int = 30) -> dict[str, object]:
    result = fetch_url(url, timeout=timeout)
    content = decode_bytes(result.get("content", b""))
    try:
        payload = json.loads(content) if content else {}
    except json.JSONDecodeError:
        payload = {}
    result["json"] = payload
    return result

#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const SCRIPT_DIR = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(SCRIPT_DIR, "..");

const DEFAULT_TARGET_URL = "https://app.wsjkw.suzhou.com.cn/ty/#/pages/index/index";
const DEFAULT_SOURCE_ID = "SZ_REG_MAP_2024";
const DEFAULT_OUT_DIR = path.join(PROJECT_ROOT, "raw_official", "registry_probe", "suzhou");
const DEFAULT_CDP_URL = "http://127.0.0.1:9222";
const TARGET_HOST = "app.wsjkw.suzhou.com.cn";
const ORG_KEYWORDS = ["托育", "机构", "幼儿园", "托儿所", "保育", "学前"];
const BANNED_CLICK_TEXT = ["机构地图", "家长端", "筛选", "搜索", "全部", "附近", "详情"];
const BROWSER_PATHS = {
  chrome: [
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
  ],
  edge: [
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
  ],
};
const HEADER_ALLOWLIST = ["accept", "content-type", "origin", "referer", "user-agent", "x-requested-with"];
const INSTITUTION_HINT_TEXT = [
  "\u6258\u80b2",
  "\u673a\u6784",
  "\u5e7c\u513f\u56ed",
  "\u6258\u513f\u6240",
  "\u4fdd\u80b2",
  "\u5b66\u524d",
  "\u513f\u7ae5",
  "\u65e9\u6559",
  "\u4e2d\u5fc3",
];
const SEARCH_LIKE_TEXT = [
  "\u8f93\u5165\u673a\u6784\u540d\u79f0",
  "\u673a\u6784\u540d\u79f0",
  "\u8bf7\u8f93\u5165",
  "\u8bf7\u8f93\u5165\u5173\u952e\u5b57",
  "\u641c\u7d22",
  "placeholder",
  "search",
];
const NOISE_MATCHERS = [
  { tag: "noise_mobileAccessToken", match: (url) => url.includes("mobileAccessToken") },
  { tag: "noise_szLoginLog_add", match: (url) => url.includes("szLoginLog/add") },
  { tag: "noise_queryDepartTreeSync", match: (url) => url.includes("queryDepartTreeSync") },
];
const URL_MARKERS = [
  { marker: "listByCondition", rank: 1, kind: "list", match: (url) => url.includes("listByCondition") },
  { marker: "listById", rank: 2, kind: "detail", match: (url) => url.includes("listById") },
  {
    marker: "/daycare/szMenuOption/getHomeList",
    rank: 3,
    kind: "list",
    match: (url) => url.includes("/daycare/szMenuOption/getHomeList"),
  },
  { marker: "/daycare/", rank: 4, kind: "detail", match: (url) => url.includes("/daycare/") },
  { marker: "/jeecg-boot/", rank: 5, kind: "platform", match: (url) => url.includes("/jeecg-boot/") },
];

function printHelp() {
  const lines = [
    "Suzhou H5 registry probe capture",
    "",
    "Usage:",
    "  node .\\scripts\\capture_suzhou_h5_probe.mjs [options]",
    "",
    "Options:",
    "  --mode persistent|cdp         Capture mode. Default: persistent",
    "  --browser chrome|edge|auto    Browser preference. Default: chrome",
    "  --cdp-url URL                 CDP endpoint for --mode cdp. Default: http://127.0.0.1:9222",
    `  --target-url URL              Page to open. Default: ${DEFAULT_TARGET_URL}`,
    `  --source-id ID                Evidence source id. Default: ${DEFAULT_SOURCE_ID}`,
    `  --out-dir PATH                Output directory. Default: ${DEFAULT_OUT_DIR}`,
    "  --headless                    Optional. Run browser headless in persistent mode",
    "  --help                        Show this help",
    "",
    "Install dependency first:",
    '  npm install --no-save --no-package-lock playwright-core',
  ];
  console.log(lines.join("\n"));
}

function parseArgs(argv) {
  const options = {
    mode: "persistent",
    browser: "chrome",
    cdpUrl: DEFAULT_CDP_URL,
    targetUrl: DEFAULT_TARGET_URL,
    sourceId: DEFAULT_SOURCE_ID,
    outDir: DEFAULT_OUT_DIR,
    headless: false,
    listReadyTimeoutMs: 25000,
    postClickWaitMs: 12000,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--headless") {
      options.headless = true;
      continue;
    }
    if (!token.startsWith("--")) {
      throw new Error(`Unknown argument: ${token}`);
    }
    const key = token.slice(2);
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`Missing value for ${token}`);
    }
    index += 1;
    switch (key) {
      case "mode":
        options.mode = value;
        break;
      case "browser":
        options.browser = value;
        break;
      case "cdp-url":
        options.cdpUrl = value;
        break;
      case "target-url":
        options.targetUrl = value;
        break;
      case "source-id":
        options.sourceId = value;
        break;
      case "out-dir":
        options.outDir = value;
        break;
      case "list-ready-timeout-ms":
        options.listReadyTimeoutMs = Number(value);
        break;
      case "post-click-wait-ms":
        options.postClickWaitMs = Number(value);
        break;
      default:
        throw new Error(`Unknown option: ${token}`);
    }
  }

  if (!["persistent", "cdp"].includes(options.mode)) {
    throw new Error(`Unsupported --mode: ${options.mode}`);
  }
  if (!["chrome", "edge", "auto"].includes(options.browser)) {
    throw new Error(`Unsupported --browser: ${options.browser}`);
  }
  options.outDir = path.resolve(options.outDir);
  new URL(options.targetUrl);
  if (options.mode === "cdp") {
    new URL(options.cdpUrl);
  }
  return options;
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
  return dirPath;
}

function writeJson(filePath, payload) {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function appendJsonl(filePath, payload) {
  fs.appendFileSync(filePath, `${JSON.stringify(payload)}\n`, "utf8");
}

function timestampNow(date = new Date()) {
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    pad(date.getMonth() + 1),
    pad(date.getDate()),
    "T",
    pad(date.getHours()),
    pad(date.getMinutes()),
    pad(date.getSeconds()),
  ].join("");
}

function sha1(text) {
  return crypto.createHash("sha1").update(text).digest("hex");
}

function cleanFileToken(text) {
  return String(text).replace(/[^A-Za-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "") || "item";
}

function detectMarker(url) {
  for (const entry of URL_MARKERS) {
    if (entry.match(url)) {
      return entry;
    }
  }
  return null;
}

function detectNoise(url) {
  for (const matcher of NOISE_MATCHERS) {
    if (matcher.match(url)) {
      return matcher.tag;
    }
  }
  return "";
}

function normalizeHeaderObject(headers) {
  const output = {};
  if (!headers || typeof headers !== "object") {
    return output;
  }
  for (const [key, value] of Object.entries(headers)) {
    output[String(key).toLowerCase()] = Array.isArray(value) ? value.join("; ") : String(value);
  }
  return output;
}

function summarizeHeaders(headers) {
  const normalized = normalizeHeaderObject(headers);
  const summary = {};
  for (const key of HEADER_ALLOWLIST) {
    if (normalized[key]) {
      summary[key] = normalized[key];
    }
  }
  summary.cookie_present = Boolean(normalized.cookie);
  return summary;
}

function isSameOriginCandidate(url, resourceType) {
  try {
    const parsed = new URL(url);
    const normalizedType = String(resourceType || "").toLowerCase();
    return parsed.host === TARGET_HOST && (normalizedType === "xhr" || normalizedType === "fetch");
  } catch {
    return false;
  }
}

function containsOrgKeywords(text) {
  return ORG_KEYWORDS.some((keyword) => text.includes(keyword));
}

function decodeBody(body, base64Encoded) {
  if (!base64Encoded) {
    return body;
  }
  try {
    return Buffer.from(body, "base64").toString("utf8");
  } catch {
    return "";
  }
}

function detectExtension(contentType, bodyText) {
  const trimmed = bodyText.trimStart();
  if (String(contentType || "").toLowerCase().includes("json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
    return ".json";
  }
  return ".txt";
}

function isJsonLike(contentType, bodyText) {
  return detectExtension(contentType, bodyText) === ".json";
}

function tryParseJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function looksLikeInstitutionName(value) {
  const text = String(value || "").trim();
  if (text.length < 4 || text.length > 60) {
    return false;
  }
  if (BANNED_CLICK_TEXT.some((item) => text.includes(item))) {
    return false;
  }
  const institutionHints = ["托育", "幼儿园", "托儿所", "保育", "学前", "儿童", "早教", "中心", "园"];
  const genericNoise = ["点击", "更多", "地图", "定位", "热线", "搜索", "全部", "附近", "健康苏州"];
  if (genericNoise.some((item) => text.includes(item))) {
    return false;
  }
  return institutionHints.some((item) => text.includes(item));
}

function looksLikeInstitutionNameStrict(value) {
  const text = String(value || "").trim();
  if (text.length < 4 || text.length > 60) {
    return false;
  }
  if (BANNED_CLICK_TEXT.some((item) => text.includes(item))) {
    return false;
  }
  if (SEARCH_LIKE_TEXT.some((item) => text.includes(item))) {
    return false;
  }
  const genericNoise = ["鐐瑰嚮", "鏇村", "鍦板浘", "瀹氫綅", "鐑嚎", "鎼滅储", "鍏ㄩ儴", "闄勮繎", "鍋ュ悍鑻忓窞"];
  if (genericNoise.some((item) => text.includes(item))) {
    return false;
  }
  return INSTITUTION_HINT_TEXT.some((item) => text.includes(item));
}

function extractInstitutionNames(payload, limit = 20) {
  const names = [];
  const visited = new Set();

  function walk(value) {
    if (names.length >= limit || value == null) {
      return;
    }
    if (typeof value === "string") {
      if (looksLikeInstitutionNameStrict(value)) {
        names.push(value.trim());
      }
      return;
    }
    if (typeof value !== "object") {
      return;
    }
    if (visited.has(value)) {
      return;
    }
    visited.add(value);
    if (Array.isArray(value)) {
      for (const item of value) {
        walk(item);
        if (names.length >= limit) {
          break;
        }
      }
      return;
    }
    for (const [key, item] of Object.entries(value)) {
      if (looksLikeInstitutionNameStrict(key)) {
        names.push(key.trim());
      }
      walk(item);
      if (names.length >= limit) {
        break;
      }
    }
  }

  walk(payload);
  return unique(names).slice(0, limit);
}

function chooseBrowserExecutable(browserPreference) {
  if (browserPreference === "auto" || browserPreference === "chrome") {
    for (const candidate of [...BROWSER_PATHS.chrome, ...BROWSER_PATHS.edge]) {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }
  if (browserPreference === "edge") {
    for (const candidate of [...BROWSER_PATHS.edge, ...BROWSER_PATHS.chrome]) {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }
  throw new Error(
    "No supported browser executable found. Expected Chrome or Edge at standard Windows paths.",
  );
}

function emptyFile(filePath) {
  fs.writeFileSync(filePath, "", "utf8");
}

function createRuntime(options) {
  const outDir = ensureDir(options.outDir);
  const responsesDir = ensureDir(path.join(outDir, "responses"));
  const metaDir = ensureDir(path.join(outDir, "meta"));
  const profileDir = ensureDir(path.join(outDir, options.mode === "persistent" ? "persistent-profile" : "cdp-profile"));
  const manifestPath = path.join(outDir, "manifest.jsonl");
  const summaryPath = path.join(outDir, "run_summary.json");
  emptyFile(manifestPath);

  return {
    options,
    startedAt: new Date().toISOString(),
    runTimestamp: timestampNow(),
    outDir,
    responsesDir,
    metaDir,
    profileDir,
    manifestPath,
    summaryPath,
    requestMap: new Map(),
    savedEntries: [],
    savedDedupKeys: new Set(),
    listNameCandidates: [],
    listResponseSeen: false,
    detailResponseSeen: false,
    captureSequence: 0,
    pageReadyReason: "",
    clickedCard: null,
    fallbackReasons: [],
  };
}

function addFallbackReason(runtime, reason) {
  if (!runtime.fallbackReasons.includes(reason)) {
    runtime.fallbackReasons.push(reason);
  }
}

function saveManifestEntry(runtime, entry) {
  runtime.savedEntries.push(entry);
  appendJsonl(runtime.manifestPath, entry);
}

function buildResponseFilePath(runtime, marker, bodyText, extension) {
  runtime.captureSequence += 1;
  const fileName = `${runtime.runTimestamp}_${String(runtime.captureSequence).padStart(2, "0")}_${cleanFileToken(marker)}_${sha1(bodyText).slice(0, 8)}${extension}`;
  return path.join(runtime.responsesDir, fileName);
}

function buildMetaFilePath(runtime, marker, requestId) {
  runtime.captureSequence += 1;
  const fileName = `${runtime.runTimestamp}_${String(runtime.captureSequence).padStart(2, "0")}_${cleanFileToken(marker)}_${cleanFileToken(requestId)}.meta.json`;
  return path.join(runtime.metaDir, fileName);
}

function recordSavedEntry(runtime, info, savedPath, bodyText, extension) {
  const parsedJson = isJsonLike(info.contentType, bodyText) ? tryParseJson(bodyText) : null;
  const entry = {
    captured_at: new Date().toISOString(),
    mode: runtime.options.mode,
    url: info.url,
    status: info.status,
    content_type: info.contentType,
    file_path: savedPath,
    contains_org_keywords: containsOrgKeywords(bodyText),
    method: info.method,
    resource_type: info.resourceType,
    marker: info.marker.marker,
    noise: Boolean(info.noiseTag),
    noise_tag: info.noiseTag,
    body_readable: true,
    body_sha1: sha1(bodyText),
    body_bytes: Buffer.byteLength(bodyText, "utf8"),
    extension,
    parseable_json: Boolean(parsedJson),
  };
  saveManifestEntry(runtime, entry);

  if (!info.noiseTag && info.marker.kind === "list") {
    runtime.listResponseSeen = true;
  }
  if (!info.noiseTag && (info.marker.kind === "detail" || info.marker.marker === "listById")) {
    runtime.detailResponseSeen = true;
  }
  if (parsedJson) {
    if (!info.noiseTag && info.marker.kind === "list") {
      runtime.listNameCandidates = unique([...runtime.listNameCandidates, ...extractInstitutionNames(parsedJson)]).slice(0, 30);
    }
  }
  const noiseLabel = info.noiseTag ? ` noise=${info.noiseTag}` : "";
  console.log(`[saved] ${info.marker.marker}${noiseLabel} -> ${savedPath}`);
}

function recordUnreadableEntry(runtime, info, errorMessage) {
  const metaPath = buildMetaFilePath(runtime, info.marker.marker, info.requestId);
  const metaPayload = {
    captured_at: new Date().toISOString(),
    source_id: runtime.options.sourceId,
    mode: runtime.options.mode,
    url: info.url,
    method: info.method,
    status: info.status,
    marker: info.marker.marker,
    noise: Boolean(info.noiseTag),
    noise_tag: info.noiseTag,
    body_read_error: errorMessage,
    request_headers_summary: summarizeHeaders(info.requestHeaders),
    response_headers_summary: summarizeHeaders(info.responseHeaders),
  };
  writeJson(metaPath, metaPayload);
  saveManifestEntry(runtime, {
    captured_at: metaPayload.captured_at,
    mode: runtime.options.mode,
    url: info.url,
    status: info.status,
    content_type: info.contentType,
    file_path: metaPath,
    contains_org_keywords: false,
    method: info.method,
    resource_type: info.resourceType,
    marker: info.marker.marker,
    noise: Boolean(info.noiseTag),
    noise_tag: info.noiseTag,
    body_readable: false,
    meta_path: metaPath,
    parseable_json: false,
  });
  addFallbackReason(runtime, `body_unreadable:${info.marker.marker}`);
  console.warn(`[meta] ${info.marker.marker} -> ${metaPath}`);
}

async function loadPlaywright() {
  try {
    return await import("playwright-core");
  } catch (error) {
    const installHint = 'npm install --no-save --no-package-lock playwright-core';
    throw new Error(`Missing dependency "playwright-core". Run: ${installHint}\n${error.message}`);
  }
}

async function pickPageForCdp(browser, targetUrl) {
  for (const context of browser.contexts()) {
    for (const page of context.pages()) {
      try {
        if (page.url() && new URL(page.url()).host === TARGET_HOST) {
          return { context, page, reused: true };
        }
      } catch {
        // ignore malformed interim page URLs
      }
    }
  }

  const context = browser.contexts()[0] || (await browser.newContext());
  const page = context.pages()[0] || (await context.newPage());
  await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
  return { context, page, reused: false };
}

async function openBrowser(runtime) {
  const { chromium } = await loadPlaywright();
  const options = runtime.options;
  if (options.mode === "persistent") {
    const executablePath = chooseBrowserExecutable(options.browser);
    console.log(`[browser] launch persistent context with ${executablePath}`);
    const context = await chromium.launchPersistentContext(runtime.profileDir, {
      executablePath,
      headless: options.headless,
      viewport: { width: 1440, height: 1024 },
      args: ["--disable-features=AutomationControlled"],
    });
    const page = context.pages()[0] || (await context.newPage());
    return { browser: null, context, page, reused: false, executablePath };
  }

  console.log(`[browser] connect over CDP ${options.cdpUrl}`);
  const browser = await chromium.connectOverCDP(options.cdpUrl);
  const picked = await pickPageForCdp(browser, options.targetUrl);
  return { browser, context: picked.context, page: picked.page, reused: picked.reused, executablePath: null };
}

function clearProbeTagsScript() {
  return `
    (() => {
      for (const node of document.querySelectorAll('[data-codex-probe-target]')) {
        node.removeAttribute('data-codex-probe-target');
      }
    })();
  `;
}

async function waitForListReady(page, runtime) {
  const started = Date.now();
  while (Date.now() - started < runtime.options.listReadyTimeoutMs) {
    const pageState = await page.evaluate(({ bannedText, institutionHints, searchLikeText }) => {
      const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
      const bodyText = normalize(document.body ? document.body.innerText : "");
      const hasMapText = bodyText.includes("机构地图");
      const candidates = [];

      function isVisible(element) {
        if (!element) {
          return false;
        }
        const style = window.getComputedStyle(element);
        if (style.visibility === "hidden" || style.display === "none" || Number(style.opacity || "1") === 0) {
          return false;
        }
        const rect = element.getBoundingClientRect();
        return rect.width >= 90 && rect.height >= 24 && rect.bottom > 40 && rect.top < window.innerHeight;
      }

      function isSearchLikeElement(element) {
        let node = element;
        while (node && node !== document.body) {
          const tagName = String(node.tagName || "").toLowerCase();
          const text = normalize(node.innerText || node.textContent || "");
          const attrs = [
            node.getAttribute ? node.getAttribute("placeholder") || "" : "",
            node.getAttribute ? node.getAttribute("aria-label") || "" : "",
            node.getAttribute ? node.getAttribute("role") || "" : "",
            node.getAttribute ? node.getAttribute("type") || "" : "",
            node.getAttribute ? node.getAttribute("class") || "" : "",
            node.getAttribute ? node.getAttribute("name") || "" : "",
          ]
            .map(normalize)
            .join(" ");
          if (tagName === "input" || tagName === "textarea" || node.isContentEditable) {
            return true;
          }
          if (searchLikeText.some((item) => text.includes(item) || attrs.includes(item))) {
            return true;
          }
          node = node.parentElement;
        }
        return false;
      }

      function isInstitutionLikeText(text) {
        return institutionHints.some((item) => text.includes(item));
      }

      for (const element of Array.from(document.querySelectorAll("body *"))) {
        if (!isVisible(element)) {
          continue;
        }
        const text = normalize(element.innerText || element.textContent || "");
        if (text.length < 4 || text.length > 40) {
          continue;
        }
        if (bannedText.some((item) => text.includes(item))) {
          continue;
        }
        if (isSearchLikeElement(element)) {
          continue;
        }
        if (!/[\\u4e00-\\u9fff]/.test(text)) {
          continue;
        }
        if (!isInstitutionLikeText(text)) {
          continue;
        }
        candidates.push(text);
        if (candidates.length >= 5) {
          break;
        }
      }

      return {
        hasMapText,
        candidateCount: candidates.length,
        bodyPreview: bodyText.slice(0, 200),
      };
    }, { bannedText: BANNED_CLICK_TEXT, institutionHints: INSTITUTION_HINT_TEXT, searchLikeText: SEARCH_LIKE_TEXT });

    if (pageState.hasMapText) {
      runtime.pageReadyReason = "page_text:机构地图";
      return;
    }
    if (runtime.listResponseSeen) {
      runtime.pageReadyReason = "network:list_response";
      return;
    }
    if (pageState.candidateCount > 0) {
      runtime.pageReadyReason = "dom:candidate_cards";
      return;
    }
    await delay(500);
  }
  addFallbackReason(runtime, "list_not_stable_in_default_mode");
  runtime.pageReadyReason = "timeout";
}

async function selectCardTarget(page, preferredNames) {
  await page.evaluate(clearProbeTagsScript());
  return page.evaluate(
    ({ preferredNames: names, bannedText, institutionHints, searchLikeText }) => {
      const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
      const preferred = names.map(normalize).filter(Boolean);

      function isVisible(element) {
        if (!element) {
          return false;
        }
        const style = window.getComputedStyle(element);
        if (style.visibility === "hidden" || style.display === "none" || Number(style.opacity || "1") === 0) {
          return false;
        }
        const rect = element.getBoundingClientRect();
        return rect.width >= 90 && rect.height >= 24 && rect.bottom > 40 && rect.top < window.innerHeight;
      }

      function isSearchLikeElement(element) {
        let node = element;
        while (node && node !== document.body) {
          const tagName = String(node.tagName || "").toLowerCase();
          const text = normalize(node.innerText || node.textContent || "");
          const attrs = [
            node.getAttribute ? node.getAttribute("placeholder") || "" : "",
            node.getAttribute ? node.getAttribute("aria-label") || "" : "",
            node.getAttribute ? node.getAttribute("role") || "" : "",
            node.getAttribute ? node.getAttribute("type") || "" : "",
            node.getAttribute ? node.getAttribute("class") || "" : "",
            node.getAttribute ? node.getAttribute("name") || "" : "",
          ]
            .map(normalize)
            .join(" ");
          if (tagName === "input" || tagName === "textarea" || node.isContentEditable) {
            return true;
          }
          if (searchLikeText.some((item) => text.includes(item) || attrs.includes(item))) {
            return true;
          }
          node = node.parentElement;
        }
        return false;
      }

      function isInstitutionLikeText(text) {
        return institutionHints.some((item) => text.includes(item));
      }

      function findClickable(element) {
        let node = element;
        while (node && node !== document.body) {
          const tagName = String(node.tagName || "").toLowerCase();
          const role = node.getAttribute ? node.getAttribute("role") : "";
          const style = window.getComputedStyle(node);
          if (
            tagName === "button" ||
            tagName === "a" ||
            role === "button" ||
            typeof node.onclick === "function" ||
            style.cursor === "pointer"
          ) {
            return node;
          }
          node = node.parentElement;
        }
        return element;
      }

      function findCardRoot(element) {
        let best = null;
        let node = element;
        while (node && node !== document.body) {
          if (isVisible(node) && !isSearchLikeElement(node)) {
            const rect = node.getBoundingClientRect();
            if (rect.width >= 140 && rect.height >= 40) {
              best = node;
            }
          }
          node = node.parentElement;
        }
        return best;
      }

      const candidates = [];
      for (const element of Array.from(document.querySelectorAll("body *"))) {
        if (!isVisible(element)) {
          continue;
        }
        const text = normalize(element.innerText || element.textContent || "");
        if (text.length < 4 || text.length > 40) {
          continue;
        }
        if (!/[\u4e00-\u9fff]/.test(text)) {
          continue;
        }
        if (bannedText.some((item) => text.includes(item))) {
          continue;
        }
        if (isSearchLikeElement(element)) {
          continue;
        }
        const rect = element.getBoundingClientRect();
        if (rect.top < 70 || rect.left > window.innerWidth - 40) {
          continue;
        }
        const isInstitutionText = isInstitutionLikeText(text);
        const cardRoot = findCardRoot(element);
        let score = 10;
        if (rect.width >= 180) {
          score += 8;
        }
        if (rect.height >= 36) {
          score += 8;
        }
        if (rect.top > 80 && rect.top < window.innerHeight - 40) {
          score += 6;
        }
        if (isInstitutionText) {
          score += 35;
        }
        if (text.includes("托育") || text.includes("幼儿园") || text.includes("托儿所")) {
          score += 12;
        }
        const matchedPreferred = preferred.find((name) => text.includes(name) || name.includes(text)) || "";
        if (matchedPreferred) {
          score += 60;
        }
        if (cardRoot) {
          score += 20;
        }
        candidates.push({
          text,
          score,
          isInstitutionText,
          matchedPreferred,
          target: findClickable(cardRoot || element),
        });
      }

      candidates.sort(
        (left, right) =>
          Number(right.isInstitutionText) - Number(left.isInstitutionText) ||
          Number(Boolean(right.matchedPreferred)) - Number(Boolean(left.matchedPreferred)) ||
          right.score - left.score ||
          left.text.length - right.text.length,
      );
      const picked = candidates[0];
      if (!picked || !picked.target) {
        return { clicked: false, reason: "no_candidate" };
      }

      picked.target.setAttribute("data-codex-probe-target", "1");
      picked.target.scrollIntoView({ block: "center", inline: "center" });
      return {
        clicked: true,
        reason: picked.matchedPreferred ? "matched_preferred_name" : "heuristic_candidate",
        text: picked.text,
        matchedPreferred: picked.matchedPreferred,
      };
    },
    {
      preferredNames,
      bannedText: BANNED_CLICK_TEXT,
      institutionHints: INSTITUTION_HINT_TEXT,
      searchLikeText: SEARCH_LIKE_TEXT,
    },
  );
}

async function clickInstitutionCard(page, runtime) {
  const preferredNames = runtime.listNameCandidates.slice(0, 10);
  const selection = await selectCardTarget(page, preferredNames);
  runtime.clickedCard = selection.clicked
    ? {
        clicked_at: new Date().toISOString(),
        text: selection.text,
        reason: selection.reason,
        matched_preferred: selection.matchedPreferred || "",
      }
    : {
        clicked_at: new Date().toISOString(),
        text: "",
        reason: selection.reason || "no_candidate",
        matched_preferred: "",
      };

  if (!selection.clicked) {
    addFallbackReason(runtime, "no_clickable_card_found");
    return false;
  }

  const locator = page.locator('[data-codex-probe-target="1"]').first();
  try {
    await locator.scrollIntoViewIfNeeded();
    await locator.click({ timeout: 4000 });
  } catch {
    await page.evaluate(() => {
      const target = document.querySelector('[data-codex-probe-target="1"]');
      if (target instanceof HTMLElement) {
        target.click();
      }
    });
  }
  console.log(`[click] ${selection.text} (${selection.reason})`);
  return true;
}

async function waitForPostClickCapture(runtime) {
  const started = Date.now();
  while (Date.now() - started < runtime.options.postClickWaitMs) {
    if (runtime.detailResponseSeen && Date.now() - started > 2500) {
      break;
    }
    await delay(500);
  }
}

function buildRequestInfo(runtime, event) {
  if (!isSameOriginCandidate(event.request?.url, event.type)) {
    return null;
  }
  const marker = detectMarker(event.request.url);
  if (!marker) {
    return null;
  }
  return {
    requestId: event.requestId,
    loaderId: event.loaderId,
    url: event.request.url,
    method: event.request.method || "GET",
    resourceType: String(event.type || "").toLowerCase(),
    marker,
    noiseTag: detectNoise(event.request.url),
    requestHeaders: normalizeHeaderObject(event.request.headers),
    responseHeaders: {},
    status: null,
    contentType: "",
    processed: false,
  };
}

async function setupNetworkCapture(page, context, runtime) {
  const session = await context.newCDPSession(page);
  await session.send("Network.enable");

  session.on("Network.requestWillBeSent", (event) => {
    const info = buildRequestInfo(runtime, event);
    if (info) {
      runtime.requestMap.set(event.requestId, info);
    }
  });

  session.on("Network.responseReceived", (event) => {
    const info = runtime.requestMap.get(event.requestId);
    if (!info) {
      return;
    }
    info.status = event.response.status;
    info.contentType = event.response.mimeType || event.response.headers?.["content-type"] || "";
    info.responseHeaders = normalizeHeaderObject(event.response.headers);
    info.url = event.response.url || info.url;
    info.noiseTag = detectNoise(info.url) || info.noiseTag;
    if (!info.noiseTag && info.marker.kind === "list") {
      runtime.listResponseSeen = true;
    }
  });

  session.on("Network.loadingFinished", async (event) => {
    const info = runtime.requestMap.get(event.requestId);
    if (!info || info.processed) {
      return;
    }
    info.processed = true;
    try {
      const bodyResult = await session.send("Network.getResponseBody", { requestId: event.requestId });
      const bodyText = decodeBody(bodyResult.body || "", bodyResult.base64Encoded);
      if (!bodyText.trim()) {
        recordUnreadableEntry(runtime, info, "empty_response_body");
        return;
      }
      const dedupKey = `${info.url}|${sha1(bodyText)}`;
      if (runtime.savedDedupKeys.has(dedupKey)) {
        return;
      }
      runtime.savedDedupKeys.add(dedupKey);

      const extension = detectExtension(info.contentType, bodyText);
      const savedPath = buildResponseFilePath(runtime, info.marker.marker, bodyText, extension);
      fs.writeFileSync(savedPath, bodyText, "utf8");
      recordSavedEntry(runtime, info, savedPath, bodyText, extension);
    } catch (error) {
      recordUnreadableEntry(runtime, info, error.message);
    }
  });

  session.on("Network.loadingFailed", (event) => {
    const info = runtime.requestMap.get(event.requestId);
    if (!info || info.processed) {
      return;
    }
    info.processed = true;
    recordUnreadableEntry(runtime, info, event.errorText || "loading_failed");
  });

  return session;
}

function chooseRecommendedJson(savedEntries) {
  const jsonEntries = savedEntries.filter(
    (entry) => entry.body_readable && entry.extension === ".json" && entry.parseable_json && !entry.noise,
  );
  if (!jsonEntries.length) {
    return { filePath: "", reason: "No parseable non-noise JSON body captured." };
  }

  const scoreOrder = [
    { marker: "listByCondition", label: "Prefer listByCondition as primary list payload." },
    { marker: "/daycare/szMenuOption/getHomeList", label: "Fallback to getHomeList list payload." },
    { marker: "listById", label: "Fallback to single-institution detail payload." },
  ];
  for (const priority of scoreOrder) {
    const candidates = jsonEntries
      .filter((entry) => entry.marker === priority.marker)
      .sort((left, right) => {
        const leftKeyword = left.contains_org_keywords ? 1 : 0;
        const rightKeyword = right.contains_org_keywords ? 1 : 0;
        if (leftKeyword !== rightKeyword) {
          return rightKeyword - leftKeyword;
        }
        return (right.body_bytes || 0) - (left.body_bytes || 0);
      });
    if (candidates.length) {
      const selected = candidates[0];
      const keywordLabel = selected.contains_org_keywords ? " Contains institution keywords." : "";
      return { filePath: selected.file_path, reason: `${priority.label}${keywordLabel}`.trim() };
    }
  }

  const fallback = [...jsonEntries].sort((left, right) => (right.body_bytes || 0) - (left.body_bytes || 0))[0];
  return { filePath: fallback.file_path, reason: "Fallback to largest parseable non-noise JSON body." };
}

function buildSummary(runtime) {
  const jsonEntries = runtime.savedEntries.filter(
    (entry) => entry.body_readable && entry.extension === ".json" && entry.parseable_json && !entry.noise,
  );
  const listCount = runtime.savedEntries.filter(
    (entry) =>
      entry.body_readable &&
      !entry.noise &&
      (entry.marker === "listByCondition" || entry.marker === "/daycare/szMenuOption/getHomeList"),
  ).length;
  const detailCount = runtime.savedEntries.filter(
    (entry) => entry.body_readable && !entry.noise && (entry.marker === "listById" || entry.marker === "/daycare/"),
  ).length;
  const recommended = chooseRecommendedJson(runtime.savedEntries);

  const onlyNoiseMarkers = runtime.savedEntries.length > 0 && listCount === 0 && detailCount === 0;
  if (!runtime.savedEntries.length || onlyNoiseMarkers) {
    addFallbackReason(runtime, "switch_to_cdp_if_default_only_captures_noise");
  }
  if (!recommended.filePath) {
    addFallbackReason(runtime, "switch_mode_and_retry_when_no_recommended_json");
  }
  if (runtime.savedEntries.some((entry) => entry.status === 200 && entry.body_readable === false)) {
    addFallbackReason(runtime, "candidate_200_but_body_unreadable");
  }

  return {
    source_id: runtime.options.sourceId,
    target_url: runtime.options.targetUrl,
    mode: runtime.options.mode,
    captured_count: runtime.savedEntries.filter((entry) => entry.body_readable).length,
    json_count: jsonEntries.length,
    detail_count: detailCount,
    list_count: listCount,
    recommended_json_for_import: recommended.filePath,
    recommended_reason: recommended.reason,
    started_at: runtime.startedAt,
    finished_at: new Date().toISOString(),
    manifest_path: runtime.manifestPath,
    page_ready_reason: runtime.pageReadyReason,
    clicked_card: runtime.clickedCard,
    list_name_candidates: runtime.listNameCandidates.slice(0, 10),
    fallback_reasons: runtime.fallbackReasons,
  };
}

async function maybeNavigate(page, runtime, reusedPage) {
  if (runtime.options.mode === "cdp" && reusedPage) {
    try {
      const currentUrl = page.url();
      if (currentUrl && new URL(currentUrl).host === TARGET_HOST) {
        console.log(`[page] reuse existing target page: ${currentUrl}`);
        await page.bringToFront();
        return;
      }
    } catch {
      // fall through to explicit goto
    }
  }
  console.log(`[page] goto ${runtime.options.targetUrl}`);
  await page.goto(runtime.options.targetUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const runtime = createRuntime(options);
  const browserSession = await openBrowser(runtime);
  const { browser, context, page, reused } = browserSession;

  let cdpSession = null;
  try {
    await page.bringToFront().catch(() => undefined);
    cdpSession = await setupNetworkCapture(page, context, runtime);
    await maybeNavigate(page, runtime, reused);
    await delay(1500);
    await waitForListReady(page, runtime);
    await clickInstitutionCard(page, runtime);
    await waitForPostClickCapture(runtime);
  } finally {
    if (cdpSession) {
      await cdpSession.detach().catch(() => undefined);
    }
    if (options.mode === "persistent") {
      await context.close().catch(() => undefined);
    }
  }

  const summary = buildSummary(runtime);
  writeJson(runtime.summaryPath, summary);
  console.log(`[summary] ${runtime.summaryPath}`);
  console.log(`recommended_json_for_import=${summary.recommended_json_for_import || ""}`);
  console.log(`recommended_reason=${summary.recommended_reason}`);
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});

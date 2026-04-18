# 三城托育数据工程 V1

本项目只服务第一阶段数据工程底座，不写论文结论，不做政策判断，不虚构数据。

## 目录约定

- `raw_official/`：官方网页、PDF、入口页、机构公开介绍页原始文件。
- `raw_api/`：高德 geocode / POI 原始 JSON。
- `clean/`：结构化清洗结果。
- `text/`：介绍文本与规则标签建议。
- `logs/`：抓取日志、blockers、人工复核列表。
- `docs/`：来源清单、人工补录模板、数据字典、AI 使用登记。
- `scripts/`：抓取、解析、清洗、编码、POI、文本规则脚本。

## 默认原则

1. 所有结构化 CSV 使用 `UTF-8-SIG`。
2. 所有 `clean/` 与 `text/` 表保留 `source_id`、`source_url`、`manual_check_flag`。
3. 抓取时优先官方页面；无法稳定自动抓取时，写 `logs/blockers.md`，并补 `docs/manual_capture_template.csv`。
4. 第一阶段不跑路径矩阵，不做复杂模型，不做情感分析。

## 推荐执行顺序

```powershell
python scripts/fetch_population_sources.py
python scripts/parse_population_tables.py
python scripts/fetch_registry_sources.py
python scripts/parse_registry_tables.py
python scripts/build_nursery_master.py
$env:AMAP_WEB_API_KEY="你的高德 key"
python scripts/geocode_addresses.py
python scripts/fetch_residential_poi.py
python scripts/fetch_support_poi.py
python scripts/build_text_tag_rules.py
python scripts/generate_data_dictionary.py
python scripts/generate_manual_review_list.py
```

## 运行说明

- 抓取脚本默认使用 Python 标准库，不依赖第三方包。
- 若官方站点证书链异常，抓取层会自动重试一次不校验证书，并把行为写入 `logs/fetch_log.csv`。
- 若页面返回 WAF、验证码、App/JS 动态入口、需要登录等情况，脚本不会硬爬，会记录 blocker。
- 高德相关脚本依赖环境变量 `AMAP_WEB_API_KEY` 或 `AMAP_KEY`。

## 当前已知难点

- 南京统计局和南京卫健委站点可能返回 WAF 拦截页，需人工补抓或换网络环境。
- 苏州托育地图是官方确认覆盖备案机构的 App 场景，第一阶段默认走人工导出/人工补录。
- 南通托育在线若页面结构不稳定，按 blocker 流程处理。

# 四城托育数据工程 V2

本项目只服务第一阶段数据工程底座，不写论文结论，不做政策判断，不虚构数据。

## 目录约定

- `raw_official/`：官方网页、PDF、入口页、机构公开介绍页原始文件。
- `raw_api/`：高德 geocode / POI 原始 JSON。
- `clean/`：结构化清洗结果。
- `text/`：介绍文本与规则标签建议。
- `logs/`：抓取日志、blockers、人工复核列表、registry minimums 报表。
- `docs/`：来源清单、人工补录模板、数据字典、AI 使用登记。
- `scripts/`：抓取、解析、清洗、编码、POI、文本规则脚本。

## 默认原则

1. 所有结构化 CSV 使用 `UTF-8-SIG`。
2. 所有 `clean/` 与 `text/` 表保留 `source_id`、`source_url`、`manual_check_flag`。
3. 抓取时优先官方页面；无法稳定自动抓取时，写 `logs/blockers.md`，并补 `docs/manual_capture_template.csv`。
4. 第一阶段不跑路径矩阵，不做复杂模型，不做情感分析。
5. registry 阶段先解决真实机构名录接入，再决定是否进入 geocode / poi。

## 推荐执行顺序

```powershell
python scripts/fetch_population_sources.py
python scripts/parse_population_tables.py

python scripts/fetch_registry_sources.py
python scripts/parse_registry_tables.py
python scripts/prepare_city_registry_tasks.py

# 人工补录过程中可反复执行
python scripts/merge_manual_capture.py
python scripts/build_nursery_master.py
python scripts/verify_registry_minimums.py

# 只有 minimums 通过后再配置高德 key
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
- `prepare_city_registry_tasks.py` 只负责把入口页线索拆成城市任务单。
- `merge_manual_capture.py` 只把已补录完成的机构行写回 `clean/nursery_registry_raw.csv`。
- `verify_registry_minimums.py` 是 registry 进入 geocode/poi 前的硬门槛。

## 当前已知难点

- 南京 registry 仍以区级公示页、`我的南京` 和 `金陵托育` 人工补录为主。
- 苏州托育地图是官方确认覆盖备案机构的 App 场景，第一阶段默认走人工导出/人工补录。
- 南通托育在线虽然公开 SSR 页面可解析，但详情页和部分字段仍需人工复核。
- 盐城已接入首条可直解析官方名录，可作为空表转实表的起点。

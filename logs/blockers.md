# blockers

记录无法稳定自动采集的来源、阻塞原因和人工补录建议。按城市归档，便于逐城清理。

## 苏州

### 2026-04-18 10:33:11 | registry_fetch | SZ_REG_MAP_2024
- source_id: SZ_REG_MAP_2024
- page_role: citywide_platform_primary
- url: https://www.suzhou.gov.cn/szsrmzf/szyw/202406/43d1eaf4085a435582b8deb958eef36e.shtml
- reason: access_method=manual_app_capture
- last_seen: 2026-04-18 10:33:11
- next_action: 该来源是 App/H5 人工流程；先保留官方入口页，再人工补录机构明细。
- resolved_flag: 0

### 2026-04-18 11:26:37 | registry_fetch | SZ_REG_MAP_2024
- source_id: SZ_REG_MAP_2024
- page_role: citywide_platform_primary
- url: https://www.suzhou.gov.cn/szsrmzf/szyw/202406/43d1eaf4085a435582b8deb958eef36e.shtml
- reason: access_method=manual_app_capture
- last_seen: 2026-04-18 11:26:37
- next_action: 该来源是 App/H5/人工浏览器补录流程；入口页已落盘后，请继续人工逐机构补录明细。
- resolved_flag: 0

## 南通

### 2026-04-18 10:33:12 | registry_fetch | NT_REG_PORTAL_2023
- source_id: NT_REG_PORTAL_2023
- page_role: official_entry
- url: https://www.nantong.gov.cn/ntsrmzf/ylfwzx/content/25923065-a8b6-41c4-9b5b-398e22870303.html
- reason: http_404
- last_seen: 2026-04-18 10:33:12
- next_action: 核对页面是否失效或迁移；保留当前入口页截图，并在同站点人工搜索新链接后补录。
- resolved_flag: 0

### 2026-04-18 11:26:37 | registry_fetch | NT_REG_PORTAL_2023
- source_id: NT_REG_PORTAL_2023
- page_role: official_entry
- url: https://www.nantong.gov.cn/ntsrmzf/ylfwzx/content/25923065-a8b6-41c4-9b5b-398e22870303.html
- reason: http_404
- last_seen: 2026-04-18 11:26:37
- next_action: 核对页面是否失效或迁移；保留当前入口页截图，并在同站点人工搜索新链接后补录。
- resolved_flag: 0

### 2026-04-18 11:32:32 | registry_fetch | NT_REG_PLATFORM_HOME
- source_id: NT_REG_PLATFORM_HOME
- page_role: platform_home
- url: https://www.health-nt.com/
- reason: access_method=manual_browser_capture
- last_seen: 2026-04-18 11:32:32
- next_action: 该来源是 App/H5/人工浏览器补录流程；入口页已落盘后，请继续人工逐机构补录明细。
- resolved_flag: 0

## 南京

### 2026-04-18 10:33:11 | registry_fetch | NJ_REG_PUHUI_2025B2
- source_id: NJ_REG_PUHUI_2025B2
- page_role: incremental_notice
- url: https://wjw.nanjing.gov.cn/njswshjhsywyh/202511/t20251111_5686527.html
- reason: http_404
- last_seen: 2026-04-18 10:33:11
- next_action: 保留阻塞截图并人工补录；若页面已迁移，按区级公示页或 App/公众号继续补录。
- resolved_flag: 0

### 2026-04-18 11:26:36 | registry_fetch | NJ_REG_PUHUI_2025B2
- source_id: NJ_REG_PUHUI_2025B2
- page_role: incremental_notice
- url: https://wjw.nanjing.gov.cn/njswshjhsywyh/202511/t20251111_5686527.html
- reason: http_404
- last_seen: 2026-04-18 11:26:36
- next_action: 核对页面是否失效或迁移；保留当前入口页截图，并在同站点人工搜索新链接后补录。
- resolved_flag: 0

### 2026-04-18 11:26:36 | registry_fetch | NJ_REG_APP_MYNJ
- source_id: NJ_REG_APP_MYNJ
- page_role: app_registry
- url: 我的南京 App > 托育服务 > 备案机构
- reason: access_method=manual_app_capture
- last_seen: 2026-04-18 11:26:36
- next_action: 该来源以 App、公众号或人工浏览器补录为主；保留入口名称和截图后，逐机构补录真实名单。
- resolved_flag: 0

### 2026-04-18 11:26:36 | registry_fetch | NJ_REG_WECHAT_JLTY
- source_id: NJ_REG_WECHAT_JLTY
- page_role: wechat_registry
- url: 金陵托育公众号 > 备案机构名单
- reason: access_method=manual_wechat_capture
- last_seen: 2026-04-18 11:26:36
- next_action: 该来源以 App、公众号或人工浏览器补录为主；保留入口名称和截图后，逐机构补录真实名单。
- resolved_flag: 0

## 盐城

## ALL

### 2026-04-18 10:31:37 | population_fetch | NJ_POP_YB_2024
- source_id: NJ_POP_YB_2024
- page_role: yearbook
- url: https://tjj.nanjing.gov.cn/material/njnj_2024/
- reason: access_method=manual_download_html
- last_seen: 2026-04-18 10:31:37
- next_action: 该来源以人工下载为主；如自动抓取结果不完整，请人工补落官方附件。
- resolved_flag: 0

### 2026-04-18 10:36:08 | geocode | AMAP_GEOCODE_DOC
- source_id: AMAP_GEOCODE_DOC
- page_role: api_reference
- url: https://lbs.amap.com/api/webservice/guide/api/georegeo
- reason: missing_api_key
- last_seen: 2026-04-18 10:36:08
- next_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 geocode_addresses.py。
- resolved_flag: 0

### 2026-04-18 10:36:08 | poi_support | AMAP_POI_DOC
- source_id: AMAP_POI_DOC
- page_role: api_reference
- url: https://lbs.amap.com/api/webservice/guide/api-advanced/search
- reason: missing_api_key
- last_seen: 2026-04-18 10:36:08
- next_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 fetch_support_poi.py。
- resolved_flag: 0

### 2026-04-18 10:36:08 | poi_residential | AMAP_POI_DOC
- source_id: AMAP_POI_DOC
- page_role: api_reference
- url: https://lbs.amap.com/api/webservice/guide/api-advanced/search
- reason: missing_api_key
- last_seen: 2026-04-18 10:36:08
- next_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 fetch_residential_poi.py。
- resolved_flag: 0

## 未分类

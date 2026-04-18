# blockers

记录无法稳定自动采集的来源、阻塞原因和人工补录建议。

## 2026-04-18 10:31:37 | population_fetch | NJ_POP_YB_2024
- city: 南京
- url: https://tjj.nanjing.gov.cn/material/njnj_2024/
- reason: access_method=manual_download_html
- manual_action: 该来源以人工下载为主；如自动抓取结果不完整，请人工补落官方附件。

## 2026-04-18 10:33:11 | registry_fetch | NJ_REG_PUHUI_2025B2
- city: 南京
- url: https://wjw.nanjing.gov.cn/njswshjhsywyh/202511/t20251111_5686527.html
- reason: http_404
- manual_action: 保留阻塞截图并人工补录；若是南京 WAF，请换网络环境或人工复制官方名单。

## 2026-04-18 10:33:11 | registry_fetch | SZ_REG_MAP_2024
- city: 苏州
- url: https://www.suzhou.gov.cn/szsrmzf/szyw/202406/43d1eaf4085a435582b8deb958eef36e.shtml
- reason: access_method=manual_app_capture
- manual_action: 该来源是 App/H5/白名单人工流程；先抓官方入口页，再人工补录机构明细。

## 2026-04-18 10:33:12 | registry_fetch | NT_REG_PORTAL_2023
- city: 南通
- url: https://www.nantong.gov.cn/ntsrmzf/ylfwzx/content/25923065-a8b6-41c4-9b5b-398e22870303.html
- reason: http_404
- manual_action: 核对页面是否失效或迁移；保留当前入口页截图，并在同站点人工搜索新链接后补录。

## 2026-04-18 10:36:08 | geocode | AMAP_GEOCODE_DOC
- city: ALL
- url: https://lbs.amap.com/api/webservice/guide/api/georegeo
- reason: missing_api_key
- manual_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 geocode_addresses.py。

## 2026-04-18 10:36:08 | poi_support | AMAP_POI_DOC
- city: ALL
- url: https://lbs.amap.com/api/webservice/guide/api-advanced/search
- reason: missing_api_key
- manual_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 fetch_support_poi.py。

## 2026-04-18 10:36:08 | poi_residential | AMAP_POI_DOC
- city: ALL
- url: https://lbs.amap.com/api/webservice/guide/api-advanced/search
- reason: missing_api_key
- manual_action: 设置环境变量 AMAP_WEB_API_KEY 后再运行 fetch_residential_poi.py。


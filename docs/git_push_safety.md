# Git 推送与 CDP 运行时治理

目标：避免 `raw_official/registry_probe/**/cdp-profile/` 及 Chrome 运行时文件进入 Git，解决 `git add .` 卡在 `Cookies` 的问题。

## 1) 工作流：安全启动 Chrome remote debugging

请使用仓库外 profile 目录，不要再放到仓库内：

```powershell
.\scripts\start_chrome_remote_debugging.ps1 -City suzhou
```

默认启动行为：
- profile 根目录：`C:\Users\12804\Desktop\统计建模_runtime\cdp_profile\`
- 城市子目录：`C:\Users\12804\Desktop\统计建模_runtime\cdp_profile\<City>`
- 端口：`9222`

如需自定义：

```powershell
.\scripts\start_chrome_remote_debugging.ps1 -City suzhou -Port 9333 -CityRuntimeBase "D:\poi_runtime" -ChromePath "C:\Program Files\Google\Chrome\Application\chrome.exe"
```

## 2) .gitignore 边界

已在 `.gitignore` 里强制忽略：
- `raw_official/registry_probe/**/cdp-profile/`
- `raw_official/registry_probe/**/persistent-profile/`
- `raw_official/registry_probe/**/User Data/`
- `raw_official/registry_probe/**/Default/`
- `raw_official/registry_probe/**/Cache/`
- `raw_official/registry_probe/**/GPUCache/`
- `raw_official/registry_probe/**/Local State`
- `raw_official/registry_probe/**/Network/`

## 3) 最小提交流程（不再用 `git add .`）

建议只提交必要文件：

```bash
git add scripts/xxx.py scripts/xxx.mjs docs/*.md .gitignore
git add clean/*.csv logs/*.csv text/*.csv
```

再配合 `git status` 复核后提交：

```bash
git status
git commit -m "chore: fix git-safe remote-debug profile layout and workflow"
```

说明：
- 生产脚本与结果文件可提交（`scripts/`, `clean/`, `logs/`, `docs/`, `.gitignore`）
- 所有浏览器运行时目录不可提交（`raw_official/registry_probe/**/cdp-profile/` 等）

param(
    [string]$City = "suzhou",
    [int]$Port = 9222,
    [string]$CityRuntimeBase,
    [string]$ChromePath
)

$repoRoot = (Get-Item $PSScriptRoot).Parent.FullName
$desktopRoot = [Environment]::GetFolderPath('Desktop')
$defaultRuntimeRoot = Join-Path $desktopRoot "统计建模_runtime"
$runtimeRoot = if ([string]::IsNullOrWhiteSpace($CityRuntimeBase)) {
    $defaultRuntimeRoot
} else {
    $CityRuntimeBase
}

$profileRoot = Join-Path $runtimeRoot "cdp_profile"
$profileDir = Join-Path $profileRoot $City

if (-not $ChromePath) {
    $candidateList = @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "$env:ProgramFiles(x86)\Google\Chrome\Application\chrome.exe",
        "$env:LocalAppData\Google\Chrome\Application\chrome.exe",
        "$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe"
    )
    $ChromePath = $candidateList | Where-Object { Test-Path $_ } | Select-Object -First 1
}

if (-not $ChromePath -or -not (Test-Path $ChromePath)) {
    Write-Error "Chrome/Edge executable not found. Please pass -ChromePath explicitly."
    exit 1
}

New-Item -ItemType Directory -Force -Path $profileDir | Out-Null
New-Item -ItemType Directory -Force -Path $runtimeRoot | Out-Null

$argsList = @(
    "--remote-debugging-port=$Port",
    "--user-data-dir=$profileDir",
    "--no-first-run",
    "--no-default-browser-check",
    "--new-window",
    "about:blank"
)

Start-Process -FilePath $ChromePath -ArgumentList $argsList

Write-Host "Chrome/Edge remote debugging started."
Write-Host "Profile: $profileDir"
Write-Host "Debug URL: http://127.0.0.1:$Port"
Write-Host "If needed, update scripts by passing -Port or -City -ChromePath explicitly."

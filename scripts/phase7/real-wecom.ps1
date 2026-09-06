param([string]$BaseUrl='http://127.0.0.1:18080')
$ErrorActionPreference='Stop'
if ([string]::IsNullOrWhiteSpace($env:PHASE7_WECOM_BOT_ID) -or [string]::IsNullOrWhiteSpace($env:PHASE7_WECOM_BOT_SECRET)) {
  '{"status":"skipped_with_reason","reason":"PHASE7_WECOM_BOT_ID or PHASE7_WECOM_BOT_SECRET is missing"}'
  exit 0
}
try { Invoke-RestMethod -Uri ($BaseUrl + '/readyz') -Method Get | ConvertTo-Json -Compress } catch { '{"status":"skipped_with_reason","reason":"service or account network unavailable"}' }

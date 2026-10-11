<#
.SYNOPSIS
  Lifecycle smoke test against the local dev stack (scripts\dev-up.ps1).

.DESCRIPTION
  Drives the real path end-to-end and prints PASS/FAIL per step:

    1. readyz                                    orchestratord healthy
    2. POST /v1/proxies                          pool proxy seeded
    3. POST /v1/bots                             bot created
    4. wait phase Running                        arm -> activate -> login -> run
    5. PATCH desiredState=paused                 clean drain + snapshot
    6. PATCH desiredState=running                resume on new epoch
    7. POST /v1/bots/{id}/restart                planned handoff
    8. taskkill the botd child                   force-kill (kill -9)
    9. wait phase Running                        crash -> auto restore
   10. DELETE /v1/bots/{id}                      teardown (skip with -KeepBot)

  A real Ed25519 admin JWT is minted with node + the panel's
  .orchestrator-dev.json -- the same signature path the panel uses.

.EXAMPLE
  scripts\dev-smoke.ps1
  scripts\dev-smoke.ps1 -BotName bot-test2 -Account bot2 -KeepBot
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
  [string]$BotName = "bot-smoke",
  [string]$Account = "bot",
  [string]$Owner = "dev-user",
  [switch]$KeepBot,
  [int]$TimeoutSec = 90
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Root  = Split-Path $PSScriptRoot -Parent
$Stack = Join-Path $Root '.devstack'
$envFile = Join-Path $Stack 'env.json'
if (-not (Test-Path $envFile)) { throw "stack not up -- run scripts\dev-up.ps1 first" }
$cfg = Get-Content $envFile -Raw | ConvertFrom-Json
$api = $cfg.api
$devFile = $cfg.panelDevFile
$creds = $cfg.accounts | Where-Object { $_.username -eq $Account }
if (-not $creds) { throw "unknown mock account $Account (dev-up seeds bot + bot2)" }

$script:passed = 0
$script:failed = 0
function Step([string]$Name, [scriptblock]$Body) {
  try {
    & $Body
    $script:passed++
    Write-Host "  PASS  $Name" -ForegroundColor Green
  } catch {
    $script:failed++
    Write-Host "  FAIL  $Name -- $_" -ForegroundColor Red
    throw
  }
}

function Invoke-Api([string]$Method, [string]$Path, $Body = $null) {
  $req = @{
    Uri             = "$api$Path"
    Method          = $Method
    Headers         = @{ Authorization = "Bearer $script:Token" }
    ContentType     = 'application/json'
    UseBasicParsing = $true
    TimeoutSec      = 15
  }
  if ($null -ne $Body) { $req.Body = ($Body | ConvertTo-Json -Depth 8 -Compress) }
  $r = Invoke-WebRequest @req
  if ($r.Content) {
    try { return ($r.Content | ConvertFrom-Json) } catch { return $r.Content }
  }
}

function Wait-Phase([string]$Id, [string]$Phase, [int]$Seconds = 90) {
  $deadline = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $deadline) {
    $b = Invoke-Api GET "/v1/bots/$Id"
    if ($b.phase -eq $Phase) { return $b }
    Start-Sleep -Seconds 2
  }
  $b = Invoke-Api GET "/v1/bots/$Id"
  throw "phase $($b.phase) != $Phase within ${Seconds}s"
}

# ------------------------------------------------------------- JWT ---------
Write-Host "==> minting admin JWT (node + panel dev keypair)" -ForegroundColor Cyan
$mintJs = @'
const fs = require("fs"), crypto = require("crypto");
const dev = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
const h = b64({ alg: "EdDSA", typ: "JWT" });
const now = Date.now();
const p = b64({ sub: "dev-user", role: "admin", act: "admin", iat: now, exp: now + 55000, jti: crypto.randomUUID() });
const key = crypto.createPrivateKey(dev.privateKey);
const sig = crypto.sign(null, Buffer.from(`${h}.${p}`), key);
console.log(`${h}.${p}.${sig.toString("base64url")}`);
'@
$mintFile = Join-Path $Stack 'mint-jwt.js'
Set-Content $mintFile $mintJs
$script:Token = (& node $mintFile $devFile).Trim()
if (-not $script:Token -or $script:Token -notmatch '\.') { throw 'JWT mint failed -- is node on PATH?' }

# ------------------------------------------------------------- run ---------
Step 'readyz' { Invoke-Api GET '/readyz' | Out-Null }
Step 'proxies: seed pool proxy' {
  $script:proxy = Invoke-Api POST '/v1/proxies' @{
    scheme = 'http'; host = '127.0.0.1'; port = [int]$cfg.proxy.Split(':')[-1]
  }
}
Step 'bots: create' {
  $script:bot = Invoke-Api POST '/v1/bots' @{
    owner = $Owner; name = $BotName
    account = @{ server = 'mock'; authMode = 'password' }
    credentials = @{ username = $creds.username; password = $creds.password }
    config = @{ settings = @{} }
    version = @{ channel = 'stable' }
    proxy = @{ mode = 'pool' }
  }
}
Step 'bot reaches Running' { Wait-Phase $BotName 'Running' $TimeoutSec | Out-Null }
Step 'pause -> Paused' {
  Invoke-Api PATCH "/v1/bots/$BotName" @{ desiredState = 'paused' } | Out-Null
  Wait-Phase $BotName 'Paused' $TimeoutSec | Out-Null
}
Step 'resume -> Running (new epoch)' {
  Invoke-Api PATCH "/v1/bots/$BotName" @{ desiredState = 'running' } | Out-Null
  Wait-Phase $BotName 'Running' $TimeoutSec | Out-Null
}
Step 'planned handoff (restart)' {
  Invoke-Api POST "/v1/bots/$BotName/restart" @{} | Out-Null
  Wait-Phase $BotName 'Running' $TimeoutSec | Out-Null
}
Step 'force-kill botd (kill -9) -> auto restore' {
  $botd = Get-CimInstance Win32_Process -Filter "Name='xenomit-botd.exe'" |
    Where-Object { $_.CommandLine -and $_.CommandLine.Contains($BotName) }
  if (-not $botd) { throw "no botd child for $BotName" }
  Stop-Process -Id $botd.ProcessId -Force
  Wait-Phase $BotName 'Running' $TimeoutSec | Out-Null
}
if (-not $KeepBot) {
  Step 'teardown (DELETE)' {
    Invoke-Api DELETE "/v1/bots/$BotName" | Out-Null
    $deadline = (Get-Date).AddSeconds(30)
    do {
      Start-Sleep -Seconds 2
      try { $b = Invoke-Api GET "/v1/bots/$BotName"; $gone = ($b.phase -eq 'Stopped') } catch { $gone = $true }
    } while (-not $gone -and (Get-Date) -lt $deadline)
  }
}

Write-Host ''
Write-Host "  $($script:passed) passed, $($script:failed) failed" -ForegroundColor $(if ($script:failed) { 'Red' } else { 'Green' })

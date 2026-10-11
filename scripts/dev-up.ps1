<#
.SYNOPSIS
  Bring up the full local Xenomit dev stack on Windows 11:
  xenomit-mock-game + xenomit-orchestratord (local mode) + web panel.

.DESCRIPTION
  - Builds the three Rust binaries with cargo (skip with -NoBuild).
  - Starts the mock game on fixed ports with two accounts
    (bot:bot-pass, bot2:bot-pass2) so two bots never share a session.
  - Starts the panel (npm run dev) with ORCHESTRATOR_URL unless -NoPanel.
  - Starts xenomit-orchestratord --mode local: real botd child processes,
    the file-backed dev Vault, and the panel-pairing link.
  - Writes .devstack\env.json + .devstack\pids\*.pid for dev-down/dev-smoke.

  Every process is killed again by scripts\dev-down.ps1.

.EXAMPLE
  scripts\dev-up.ps1
  scripts\dev-up.ps1 -NoPanel
  scripts\dev-up.ps1 -ApiPort 9080 -GamePort 6611
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
  [switch]$NoPanel,
  [switch]$NoBuild,
  [int]$ApiPort = 8080,
  [int]$GatewayPort = 8443,
  [int]$VaultPort = 8200,
  [int]$BackpagePort = 5610,
  [int]$GamePort = 5611,
  [int]$ProxyPort = 5612,
  [int]$ControlPort = 5613,
  [int]$PanelPort = 3000
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Root    = Split-Path $PSScriptRoot -Parent          # xenomit-orchestrator
$Repos   = Split-Path $Root -Parent                  # repos root
$Stack   = Join-Path $Root '.devstack'
$Logs    = Join-Path $Stack 'logs'
$Pids    = Join-Path $Stack 'pids'
$PanelDir = Join-Path $Repos 'xenomit-web-panel'
$MockExe  = Join-Path $Repos 'xenomit-mock-game\target\debug\xenomit-mock-game.exe'
$BotdExe  = Join-Path $Repos 'xenomit-botd\target\debug\xenomit-botd.exe'
$DExe     = Join-Path $Root 'target\debug\xenomit-orchestratord.exe'

function Write-Step([string]$Msg) { Write-Host "==> $Msg" -ForegroundColor Cyan }
function Write-Ok([string]$Msg)   { Write-Host "    $Msg" -ForegroundColor Green }

function Test-PortFree([int]$Port) {
  $c = New-Object Net.Sockets.TcpClient
  try { $c.Connect('127.0.0.1', $Port); $c.Close(); return $false }
  catch { return $true }
}

function Wait-Url([string]$Url, [int]$Seconds = 30, [string]$What = 'endpoint') {
  $deadline = (Get-Date).AddSeconds($Seconds)
  while ((Get-Date) -lt $deadline) {
    try {
      $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
      if ($r.StatusCode -lt 500) { return $true }
    } catch { Start-Sleep -Milliseconds 500 }
  }
  throw "$What did not answer at $Url within ${Seconds}s"
}

function Start-Logged([string]$Exe, [string[]]$ArgList, [string]$Name, [string]$WorkDir) {
  $out = Join-Path $Logs "$Name.out.log"
  $err = Join-Path $Logs "$Name.err.log"
  $p = Start-Process -FilePath $Exe -ArgumentList $ArgList -WorkingDirectory $WorkDir `
       -RedirectStandardOutput $out -RedirectStandardError $err `
       -WindowStyle Hidden -PassThru
  Set-Content -Path (Join-Path $Pids "$Name.pid") -Value $p.Id
  return $p
}

New-Item -ItemType Directory -Force -Path $Logs, $Pids | Out-Null

# -------------------------------------------------------------- build ------
function Invoke-CargoBuild([string[]]$CargoArgs, [string]$LogName, [string]$WorkDir) {
  $log = Join-Path $Logs $LogName
  # Redirect to a file: native stderr through the PS pipeline throws
  # NativeCommandError when ErrorActionPreference is Stop.
  Push-Location $WorkDir
  try {
    # cmd /c so cmd owns stderr redirection -- a native command's stderr
    # surfaced through PowerShell throws NativeCommandError under EAP=Stop.
    & cmd /c "cargo $($CargoArgs -join ' ') > `"$log`" 2>&1"
  } finally { Pop-Location }
  if ($LASTEXITCODE -ne 0) { throw "cargo build failed ($LogName) -- see $log" }
}

if (-not $NoBuild) {
  Write-Step 'cargo build xenomit-mock-game'
  Invoke-CargoBuild @('build', '--manifest-path', (Join-Path $Repos 'xenomit-mock-game\Cargo.toml')) 'build-mock.log' $Repos
  Write-Step 'cargo build xenomit-botd'
  Invoke-CargoBuild @('build', '--manifest-path', (Join-Path $Repos 'xenomit-botd\Cargo.toml')) 'build-botd.log' $Repos
  Write-Step 'cargo build -p orchestratord'
  Invoke-CargoBuild @('build', '-p', 'orchestratord') 'build-orchestratord.log' $Root
}
foreach ($exe in @($MockExe, $BotdExe, $DExe)) {
  if (-not (Test-Path $exe)) { throw "missing binary $exe -- run without -NoBuild" }
}

# --------------------------------------------------------- port checks -----
$ports = @{ api = $ApiPort; gateway = $GatewayPort; vault = $VaultPort;
            backpage = $BackpagePort; game = $GamePort; proxy = $ProxyPort;
            control = $ControlPort }
if (-not $NoPanel) { $ports.panel = $PanelPort }
foreach ($k in $ports.Keys) {
  if (-not (Test-PortFree $ports[$k])) {
    throw "port $($ports[$k]) ($k) is already in use -- run scripts\dev-down.ps1 or pass different -$($k)Port"
  }
}

# ---------------------------------------------------------- mock game ------
Write-Step 'starting xenomit-mock-game'
$mockArgs = @(
  '--backpage', "127.0.0.1:$BackpagePort",
  '--game',     "127.0.0.1:$GamePort",
  '--proxy',    "127.0.0.1:$ProxyPort",
  '--control',  "127.0.0.1:$ControlPort",
  '--account',  'bot:bot-pass:1001',
  '--account',  'bot2:bot-pass2:1002'
)
Start-Logged $MockExe $mockArgs 'mock-game' (Join-Path $Repos 'xenomit-mock-game') | Out-Null
Wait-Url "http://127.0.0.1:$ControlPort/status" 30 'mock control API' | Out-Null
Write-Ok "mock game: backpage http://127.0.0.1:$BackpagePort  game 127.0.0.1:$GamePort  proxy 127.0.0.1:$ProxyPort"

# ------------------------------------------------------------- panel -------
$devFile = Join-Path $PanelDir '.orchestrator-dev.json'
if (-not $NoPanel) {
  if (-not (Test-Path (Join-Path $PanelDir 'package.json'))) {
    Write-Warning "panel not found at $PanelDir -- continuing without it"
    $NoPanel = $true
  }
}
if (-not $NoPanel) {
  Write-Step 'starting xenomit-web-panel (npm run dev)'
  # cmd wrapper so npm's .cmd shim resolves; env set on OUR process is
  # inherited by the child.
  $env:ORCHESTRATOR_URL = "http://127.0.0.1:$ApiPort"
  Start-Logged 'cmd.exe' @('/c', 'npm run dev') 'panel' $PanelDir | Out-Null
  $deadline = (Get-Date).AddSeconds(60)
  while (-not (Test-Path $devFile) -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 500 }
  if (-not (Test-Path $devFile)) { throw 'panel did not write .orchestrator-dev.json within 60s' }
  Wait-Url "http://127.0.0.1:$PanelPort" 60 'panel' | Out-Null
  Write-Ok "panel: http://127.0.0.1:$PanelPort (dev login enabled via panel .env)"
}

if (-not (Test-Path $devFile)) {
  throw "no JWT key: run once with the panel (it generates $devFile) or pass a key -- see GETTING-STARTED.md"
}

# ------------------------------------------------------ orchestratord ------
Write-Step 'starting xenomit-orchestratord (local mode)'
$dArgs = @(
  '--mode', 'local',
  '--data-dir', (Join-Path $Stack 'orchestratord'),
  '--api-bind', "127.0.0.1:$ApiPort",
  '--gateway-bind', "127.0.0.1:$GatewayPort",
  '--vault-bind', "127.0.0.1:$VaultPort",
  '--botd', $BotdExe,
  '--panel-dev-file', $devFile,
  '--botd-env', "XENOMIT_TESTING_BACKPAGE_URL=http://127.0.0.1:$BackpagePort",
  '--botd-env', "XENOMIT_TESTING_GAME_ENDPOINT=127.0.0.1:$GamePort"
)
if (-not $NoPanel) { $dArgs += @('--panel-url', "http://127.0.0.1:$PanelPort") }
Start-Logged $DExe $dArgs 'orchestratord' $Root | Out-Null
Wait-Url "http://127.0.0.1:$ApiPort/livez" 30 'orchestrator API' | Out-Null
Wait-Url "http://127.0.0.1:$ApiPort/readyz" 30 'orchestrator API readyz' | Out-Null
Write-Ok "orchestratord: api http://127.0.0.1:$ApiPort  gateway 127.0.0.1:$GatewayPort  dev-vault http://127.0.0.1:$VaultPort"

# -------------------------------------------------------------- state ------
$envFile = Join-Path $Stack 'env.json'
@{
  api        = "http://127.0.0.1:$ApiPort"
  gateway    = "127.0.0.1:$GatewayPort"
  vault      = "http://127.0.0.1:$VaultPort"
  backpage   = "http://127.0.0.1:$BackpagePort"
  game       = "127.0.0.1:$GamePort"
  proxy      = "127.0.0.1:$ProxyPort"
  control    = "http://127.0.0.1:$ControlPort"
  panel      = if ($NoPanel) { $null } else { "http://127.0.0.1:$PanelPort" }
  panelDevFile = $devFile
  accounts   = @(
    @{ username = 'bot';  password = 'bot-pass';  userId = 1001 },
    @{ username = 'bot2'; password = 'bot-pass2'; userId = 1002 }
  )
} | ConvertTo-Json -Depth 4 | Set-Content $envFile

Write-Host ''
Write-Step 'stack is up'
Write-Host @"

  API:        http://127.0.0.1:$ApiPort    (/api/v1 + /v1 + /livez)
  Gateway:    127.0.0.1:$GatewayPort       (bot control stream)
  Dev Vault:  http://127.0.0.1:$VaultPort  (file-backed, dev only)
  Mock:       backpage :$BackpagePort  game :$GamePort  proxy :$ProxyPort  control :$ControlPort
  Panel:      $(if ($NoPanel) { 'disabled' } else { "http://127.0.0.1:$PanelPort" })
  Accounts:   bot/bot-pass (uid 1001), bot2/bot-pass2 (uid 1002)

  Smoke test: scripts\dev-smoke.ps1
  Shutdown:   scripts\dev-down.ps1
  Logs:       $Logs   (botd children land here too)

"@

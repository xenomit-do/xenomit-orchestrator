<#
.SYNOPSIS
  Tear down the local Xenomit dev stack started by scripts\dev-up.ps1.

.DESCRIPTION
  1. Kills every xenomit-botd.exe child whose --config path lives under
     .devstack (dev-owned processes only -- a stray user-run botd is left
     alone).
  2. Kills the panel / orchestratord / mock-game processes recorded in
     .devstack\pids, including their child trees (taskkill /T).
  3. Removes the pid files and dead-pod state (rendered configs under
     pods\, epoch fencing files under botd-state\epochs -- epochs restart
     at 1 on the next dev-up, so a leftover watermark would fence every
     fresh pod into a silent exit 67). Vault secrets and the
     snapshot/statistics archives are kept; pass -Clean to remove all of
     .devstack.

.EXAMPLE
  scripts\dev-down.ps1
  scripts\dev-down.ps1 -Clean
#>
#Requires -Version 5.1
[CmdletBinding()]
param([switch]$Clean)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

$Root  = Split-Path $PSScriptRoot -Parent
$Stack = Join-Path $Root '.devstack'
$Pids  = Join-Path $Stack 'pids'

function Write-Step([string]$Msg) { Write-Host "==> $Msg" -ForegroundColor Cyan }

function Stop-Tree([int]$ProcessId) {
  # /T takes the whole tree (cmd -> npm -> node, orchestratord -> botd).
  $null = & taskkill /PID $ProcessId /T /F 2>&1
}

# ---- 1. botd children still running under .devstack -------------------------
Write-Step 'stopping dev-owned xenomit-botd processes'
$botd = Get-CimInstance Win32_Process -Filter "Name='xenomit-botd.exe'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine.Contains($Stack) }
foreach ($p in $botd) {
  Write-Host "    stopping botd pid $($p.ProcessId)"
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
if (-not $botd) { Write-Host '    (none running)' }

# ---- 2. recorded pids --------------------------------------------------------
Write-Step 'stopping recorded processes'
foreach ($name in 'orchestratord', 'panel', 'mock-game') {
  $file = Join-Path $Pids "$name.pid"
  if (-not (Test-Path $file)) { continue }
  $procId = [int](Get-Content $file -Raw).Trim()
  $alive = Get-Process -Id $procId -ErrorAction SilentlyContinue
  if ($alive) {
    Write-Host "    stopping $name pid $procId (tree)"
    Stop-Tree $procId
  } else {
    Write-Host "    $name pid $procId already gone"
  }
  Remove-Item $file -Force -ErrorAction SilentlyContinue
}

# A panel started through `cmd /c npm run dev` detaches node.exe children;
# taskkill /T covers it, but sweep orphaned tsx/node processes that still
# hold port 3000 just in case.
$nodeLeft = Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine.Contains('xenomit-web-panel') }
foreach ($p in $nodeLeft) {
  Write-Host "    stopping panel node pid $($p.ProcessId)"
  Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}

# ---- 3. dead-pod state --------------------------------------------------------
# Rendered pod configs and epoch fencing files only describe pods that just
# died -- they must not outlive the control plane that minted their epochs.
$orchState = Join-Path $Stack 'orchestratord'
foreach ($sub in 'pods', 'botd-state\epochs') {
  $p = Join-Path $orchState $sub
  if (Test-Path $p) {
    Write-Host "    removing dead-pod state $sub"
    Remove-Item $p -Recurse -Force -ErrorAction SilentlyContinue
  }
}

# ---- 4. retained state ---------------------------------------------------------
if ($Clean) {
  Write-Step "removing $Stack"
  Remove-Item $Stack -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Step 'done'

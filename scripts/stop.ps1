# scripts/stop.ps1
# Stops backend + frontend started by start.ps1

$ErrorActionPreference = "SilentlyContinue"

$ROOT = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$PID_DIR = Join-Path $ROOT ".pids"

function Stop-FromPidFile($pidFile) {
  if (!(Test-Path $pidFile)) { return }
  $pid = Get-Content $pidFile -ErrorAction SilentlyContinue
  if ($pid) {
    Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue
  }
  Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}

Stop-FromPidFile (Join-Path $PID_DIR "frontend.pid")
Stop-FromPidFile (Join-Path $PID_DIR "backend.pid")

# scripts/start.ps1
# Starts backend + frontend in the background and opens the browser.
# Run via start_hidden.vbs so no terminal window is required.

$ErrorActionPreference = "Stop"

$ROOT = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$BACKEND_DIR = Join-Path $ROOT "backend"
$FRONTEND_DIR = Join-Path $ROOT "frontend"

# ─────────────────────────────────────────────
# Use localhost to preserve browser localStorage
# ─────────────────────────────────────────────
$BACKEND_HOST = "localhost"
$FRONTEND_HOST = "localhost"

$BACKEND_PORT = 8000
$FRONTEND_PORT = 5173
$FRONTEND_URL = "http://${FRONTEND_HOST}:${FRONTEND_PORT}"

$PID_DIR = Join-Path $ROOT ".pids"
New-Item -ItemType Directory -Force -Path $PID_DIR | Out-Null

function Start-IfNotRunning($name, $cmd, $workdir, $pidFile) {
  if (Test-Path $pidFile) {
    $pid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($pid) {
      $p = Get-Process -Id $pid -ErrorAction SilentlyContinue
      if ($p) { return }
    }
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
  }

  $p = Start-Process `
    -WindowStyle Hidden `
    -PassThru `
    -WorkingDirectory $workdir `
    -FilePath "cmd.exe" `
    -ArgumentList "/c", $cmd

  Set-Content -Path $pidFile -Value $p.Id
}

# Backend: prefer venv python
$venvPy = Join-Path $BACKEND_DIR ".venv\Scripts\python.exe"
$py = (Test-Path $venvPy) ? $venvPy : "python"

$backendCmd  = "$py -m uvicorn app.main:app --host $BACKEND_HOST --port $BACKEND_PORT"
$frontendCmd = "npm run dev -- --host $FRONTEND_HOST --port $FRONTEND_PORT"

Start-IfNotRunning "backend"  $backendCmd  $BACKEND_DIR  (Join-Path $PID_DIR "backend.pid")
Start-IfNotRunning "frontend" $frontendCmd $FRONTEND_DIR (Join-Path $PID_DIR "frontend.pid")

Start-Sleep -Seconds 1
Start-Process $FRONTEND_URL

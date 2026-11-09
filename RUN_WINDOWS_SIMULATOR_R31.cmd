@echo off
setlocal
cd /d "%~dp0"
if defined OCPP_SIM_PYTHON (
  set "SIM_PYTHON=%OCPP_SIM_PYTHON%"
) else if exist "%~dp0.venv\Scripts\python.exe" (
  set "SIM_PYTHON=%~dp0.venv\Scripts\python.exe"
) else (
  echo Create the simulator environment first:
  echo py -m venv .venv
  echo .venv\Scripts\python.exe -m pip install -r requirements-simulator.txt
  pause
  exit /b 1
)
if not exist "%SIM_PYTHON%" exit /b 1
echo R3.1 SIMULATED charger: separate TEST IDs, no physical charger hardware.
"%SIM_PYTHON%" -u live_dual_simulator.py --config dual_csms_simulator_config.json --state-dir simulator_state_R31 --full-protocol
echo Simulator exited. Preserve state and inspect pending transactions before retrying.
pause

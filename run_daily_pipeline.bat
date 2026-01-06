@echo off
setlocal

REM === Se placer automatiquement dans le dossier du .bat ===
cd /d "%~dp0"

docker compose up -d

docker exec -i orchestrator python /app/scripts/generate_daily_files.py
docker exec -i orchestrator python /app/scripts/run_pipeline_hdfs.py

endlocal
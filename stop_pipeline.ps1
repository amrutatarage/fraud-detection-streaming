Write-Host "Teardown Initiated: Fraud Detection Pipeline..." -ForegroundColor Yellow

# Simply use docker compose down.
# This prevents wiping the managed grafana-storage volume.
docker compose down

Write-Host "Pipeline safely offline. Data volumes like Grafana dashboards are preserved." -ForegroundColor Green

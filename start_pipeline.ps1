Write-Host "Starting Fraud Detection Enterprise Pipeline..." -ForegroundColor Cyan

# Pull down existing containers safely (without destroying volumes like Grafana dashboards)
docker compose down

# Bring up clusters and natively reconstruct any image overlays
Write-Host "Building and launching infrastructure layers..." -ForegroundColor Cyan
docker compose up -d --build

# Health buffer to ensure the Kafka brokers and Postgres schemas are successfully negotiated
Write-Host "Waiting 30 seconds for Kafka and Postgres clusters to stabilize..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Exectue Flink job into the background
Write-Host "Submitting Advanced PyFlink Job into cluster memory..." -ForegroundColor Green
docker exec -d flink-jobmanager bash -c "python /opt/flink/jobs/fraud_detection_v3.py > /opt/flink/fraud_runtime.log 2>&1"

Write-Host "===========================================================" -ForegroundColor Cyan
Write-Host "Pipeline is now RUNNING! 🚀" -ForegroundColor Green
Write-Host "Producer: Generating up to 500 TPS asynchronously with Card-Testing and Velocity Anomalies."
Write-Host "Grafana: http://localhost:3000 (admin/admin)"
Write-Host ""
Write-Host "To monitor Producer output:"
Write-Host "  docker logs -f fraud-producer"
Write-Host ""
Write-Host "To monitor real-time Flink output:"
Write-Host "  docker exec -it flink-jobmanager tail -f /opt/flink/fraud_runtime.log"
Write-Host "===========================================================" -ForegroundColor Cyan

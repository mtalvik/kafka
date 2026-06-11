Write-Host "Docker containers:" -ForegroundColor Cyan
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host ""
Write-Host "Images used by ZooKeeper compose:" -ForegroundColor Cyan
Select-String -Path .\docker-compose.zookeeper.yml -Pattern "image:"

Write-Host ""
Write-Host "Images used by KRaft compose:" -ForegroundColor Cyan
Select-String -Path .\docker-compose.kraft.yml -Pattern "image:"

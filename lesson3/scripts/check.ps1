docker ps
Write-Host ""
Write-Host "Images in ZooKeeper compose:"
Select-String -Path .\docker-compose.zookeeper.yml -Pattern "image:"

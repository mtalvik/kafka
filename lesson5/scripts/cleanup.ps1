Write-Host "Останавливаем demo stack и удаляем контейнеры" -ForegroundColor Cyan
docker compose down -v --remove-orphans
Write-Host "Готово" -ForegroundColor Green

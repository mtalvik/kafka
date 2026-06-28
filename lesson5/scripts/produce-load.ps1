param(
    [int]$Count = 1000,
    [string]$Topic = "orders"
)

$NoJmx = 'unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT; '

Write-Host "Отправляем $Count сообщений в topic $Topic" -ForegroundColor Cyan
1..$Count | ForEach-Object { "order_id=$($_);status=load-test;payload=$(New-Guid)" } | docker compose exec -T kafka bash -lc "$NoJmx kafka-console-producer --bootstrap-server kafka:9092 --topic $Topic"
Write-Host "Готово" -ForegroundColor Green

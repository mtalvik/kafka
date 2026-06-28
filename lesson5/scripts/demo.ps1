$NoJmx = 'unset KAFKA_JMX_OPTS KAFKA_JMX_PORT JMX_PORT; '

Write-Host "Kafka Monitoring Demo: creating topic orders" -ForegroundColor Cyan
docker compose exec kafka bash -lc "$NoJmx kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1"

Write-Host "Showing the list of topics" -ForegroundColor Cyan
docker compose exec kafka bash -lc "$NoJmx kafka-topics --bootstrap-server kafka:9092 --list"

Write-Host "Sending 100 messages to the orders topic" -ForegroundColor Cyan
1..100 | ForEach-Object { "order_id=$($_);status=created;ts=$(Get-Date -Format o)" } | docker compose exec -T kafka bash -lc "$NoJmx kafka-console-producer --bootstrap-server kafka:9092 --topic orders"

Write-Host "Reading 10 messages with consumer group demo-group so a committed offset appears" -ForegroundColor Cyan
docker compose exec kafka bash -lc "$NoJmx kafka-console-consumer --bootstrap-server kafka:9092 --topic orders --group demo-group --from-beginning --max-messages 10"

Write-Host "Sending another 200 messages. demo-group should now have lag" -ForegroundColor Cyan
101..300 | ForEach-Object { "order_id=$($_);status=created;ts=$(Get-Date -Format o)" } | docker compose exec -T kafka bash -lc "$NoJmx kafka-console-producer --bootstrap-server kafka:9092 --topic orders"

Write-Host "Showing lag with kafka-consumer-groups" -ForegroundColor Cyan
docker compose exec kafka bash -lc "$NoJmx kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group demo-group"

Write-Host "Done. Open Grafana: http://localhost:3000  login admin / password admin" -ForegroundColor Green
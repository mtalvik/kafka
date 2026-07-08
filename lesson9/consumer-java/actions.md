`1.` Запуск контейнеров Docker:
```shell
docker compose up -d
```

`2.` Вывод списока всех контейнеров Docker (включая остановленные):
```shell
docker compose ps -a
```

`3.` Получение списка топиков которые есть в Kafka брокере
```shell
docker exec -ti kafka1 /usr/bin/kafka-topics --list --bootstrap-server localhost:9191
```

`4.` Создание топика "topic3" с тремя партициями
```shell
docker exec -ti kafka1 /usr/bin/kafka-topics --create --topic topic3 --partitions 3 --replication-factor 1 --bootstrap-server localhost:9191
```

`5.` Останавливаем контейнеры, удаляем контейнеры, удаляем неиспользуемые тома:
```shell
docker compose stop
docker container prune -f
docker volume prune -f
```

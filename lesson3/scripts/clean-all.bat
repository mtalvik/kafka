@echo off
docker compose -f docker-compose.zookeeper.yml down -v --remove-orphans
docker compose -f docker-compose.kraft.yml down -v --remove-orphans
docker rm -f kafka1 kafka2 kafka3 zookeeper kafka-ui kraft-kafka1 kraft-kafka2 kraft-kafka3 kraft-kafka-ui 2>nul
docker system prune -f

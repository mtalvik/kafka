docker compose -f docker-compose.kraft.yml ps
docker exec kafka1-kraft kafka-topics.sh --bootstrap-server kafka1:9092 --list

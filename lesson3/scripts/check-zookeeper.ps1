docker compose -f docker-compose.zookeeper.yml ps
docker exec kafka1-zk kafka-topics.sh --bootstrap-server kafka1:9092 --list
